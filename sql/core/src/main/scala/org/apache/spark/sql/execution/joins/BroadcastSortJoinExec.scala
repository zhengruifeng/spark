/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import java.io._
import java.util.UUID

import scala.collection.mutable

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkFiles, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.{CompletionIterator, Utils}


/**
 * Performs an inner hash join of two child relations. This build relation is placed in a
 * broadcast sorted file. The streamed relation is not shuffled, but locally sorted.
 */
case class BroadcastSortJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BaseJoinExec with CodegenSupport {
  import BroadcastSortJoinExec._

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def requiredChildDistribution: Seq[Distribution] = {
    // require buildPlan to be GLOBAL SORTED, and streamedPlan to be SORTED within each partition.
    buildSide match {
      case BuildLeft =>
        OrderedDistribution(requiredOrders(leftKeys)) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: OrderedDistribution(requiredOrders(rightKeys)) :: Nil
    }
  }

  /**
   * current file broadcasting is simply based on Spark File Server.
   *
   * TODO: move it to BroadcastExchangeExec
   */
  private def doBroadcast(): Array[String] = {
    val buildFile = s"${sparkContext.applicationId}_bsj_${id}_build_file_${UUID.randomUUID}"
    val (buildPath: String, os: OutputStream) = sparkContext.getCheckpointDir match {
      case Some(dir) =>
        // TODO: directly write HDFS file to alleviate driver's workload
        val buildPath = s"$dir/$buildFile"
        val fs = FileSystem.get(sparkContext.hadoopConfiguration)
        (buildPath, fs.create(new Path(buildPath)))

      case None =>
        logWarning("Checkpoint directory NOT available, fallback to driver's local directory.")
        val buildPath = s"${Utils.createTempDir()}/$buildFile"
        (buildPath, new FileOutputStream(buildPath))
    }

    logInfo(s"starting to write build file: $buildPath")
    val timestamp1 = System.nanoTime
    val oos = new ObjectOutputStream(os)
    val buildIter = buildPlan.execute().map(_.copy()).toLocalIterator
    var numRows = 0L
    while (buildIter.hasNext) {
      // TODO: file compression
      oos.writeObject(buildIter.next)
      numRows += 1L
    }
    oos.close()
    os.close()
    val timestamp2 = System.nanoTime
    logInfo(s"finish writing build file: $buildPath, totally $numRows rows, " +
      s"duration: ${(timestamp2 - timestamp1) / 1e9} sec")

    logInfo(s"starting to broadcast build file: $buildPath")
    sparkContext.addFile(buildPath)
    val timestamp3 = System.nanoTime
    logInfo(s"finish broadcast build file: $buildPath, " +
      s"duration: ${(timestamp3 - timestamp2) / 1e9} sec")
    // TODO: when downloading a big file on a worker, all active tasks hang
    // TODO: should use a sequence of file splits of appropriate size
    Array(buildFile)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val spillThreshold = getSpillThreshold
    val inMemoryThreshold = getInMemoryThreshold

    val buildFiles = doBroadcast()
    streamedPlan.execute().mapPartitionsInternal { streamIter =>
      val pid = TaskContext.get().partitionId()
      val buildIter = createInternalRowIterator(buildFiles.map(SparkFiles.get))

      val (leftIter, rightIter) = buildSide match {
        case BuildLeft => (buildIter, streamIter)
        case BuildRight => (streamIter, buildIter)
      }

      // [InnerLike/LeftOuter/RightOuter/LeftSemi/LeftAnti/ExistenceJoin] copied
      // from SortMergeJoinExec, [FullOuter] is excluded.
      val boundCondition: (InternalRow) => Boolean = {
        condition.map { cond =>
          Predicate.create(cond, left.output ++ right.output).eval _
        }.getOrElse {
          (r: InternalRow) => true
        }
      }

      // An ordering that can be used to compare keys from both sides.
      val keyOrdering = RowOrdering.createNaturalAscendingOrdering(leftKeys.map(_.dataType))
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      joinType match {
        case _: InnerLike =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] var currentRightMatches: ExternalAppendOnlyUnsafeRowArray = _
            private[this] var rightMatchesIterator: Iterator[UnsafeRow] = null
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              cleanupResources
            )
            private[this] val joinRow = new JoinedRow

            if (smjScanner.findNextInnerJoinRows()) {
              currentRightMatches = smjScanner.getBufferedMatches
              currentLeftRow = smjScanner.getStreamedRow
              rightMatchesIterator = currentRightMatches.generateIterator()
            }

            override def advanceNext(): Boolean = {
              while (rightMatchesIterator != null) {
                if (!rightMatchesIterator.hasNext) {
                  if (smjScanner.findNextInnerJoinRows()) {
                    currentRightMatches = smjScanner.getBufferedMatches
                    currentLeftRow = smjScanner.getStreamedRow
                    rightMatchesIterator = currentRightMatches.generateIterator()
                  } else {
                    currentRightMatches = null
                    currentLeftRow = null
                    rightMatchesIterator = null
                    return false
                  }
                }
                joinRow(currentLeftRow, rightMatchesIterator.next())
                if (boundCondition(joinRow)) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = resultProj(joinRow)
          }.toScala

        case LeftOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createLeftKeyGenerator(),
            bufferedKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(leftIter),
            bufferedIter = RowIterator.fromScala(rightIter),
            inMemoryThreshold,
            spillThreshold,
            cleanupResources
          )
          val rightNullRow = new GenericInternalRow(right.output.length)
          new LeftOuterIterator(
            smjScanner, rightNullRow, boundCondition, resultProj, numOutputRows).toScala

        case RightOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createRightKeyGenerator(),
            bufferedKeyGenerator = createLeftKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(rightIter),
            bufferedIter = RowIterator.fromScala(leftIter),
            inMemoryThreshold,
            spillThreshold,
            cleanupResources
          )
          val leftNullRow = new GenericInternalRow(left.output.length)
          new RightOuterIterator(
            smjScanner, leftNullRow, boundCondition, resultProj, numOutputRows).toScala

        case LeftSemi =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              cleanupResources,
              condition.isEmpty
            )
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextInnerJoinRows()) {
                val currentRightMatches = smjScanner.getBufferedMatches
                currentLeftRow = smjScanner.getStreamedRow
                if (currentRightMatches != null && currentRightMatches.length > 0) {
                  val rightMatchesIterator = currentRightMatches.generateIterator()
                  while (rightMatchesIterator.hasNext) {
                    joinRow(currentLeftRow, rightMatchesIterator.next())
                    if (boundCondition(joinRow)) {
                      numOutputRows += 1
                      return true
                    }
                  }
                }
              }
              false
            }

            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case LeftAnti =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              cleanupResources,
              condition.isEmpty
            )
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) {
                currentLeftRow = smjScanner.getStreamedRow
                val currentRightMatches = smjScanner.getBufferedMatches
                if (currentRightMatches == null || currentRightMatches.length == 0) {
                  numOutputRows += 1
                  return true
                }
                var found = false
                val rightMatchesIterator = currentRightMatches.generateIterator()
                while (!found && rightMatchesIterator.hasNext) {
                  joinRow(currentLeftRow, rightMatchesIterator.next())
                  if (boundCondition(joinRow)) {
                    found = true
                  }
                }
                if (!found) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case j: ExistenceJoin =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val result: InternalRow = new GenericInternalRow(Array[Any](null))
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter),
              inMemoryThreshold,
              spillThreshold,
              cleanupResources,
              condition.isEmpty
            )
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) {
                currentLeftRow = smjScanner.getStreamedRow
                val currentRightMatches = smjScanner.getBufferedMatches
                var found = false
                if (currentRightMatches != null && currentRightMatches.length > 0) {
                  val rightMatchesIterator = currentRightMatches.generateIterator()
                  while (!found && rightMatchesIterator.hasNext) {
                    joinRow(currentLeftRow, rightMatchesIterator.next())
                    if (boundCondition(joinRow)) {
                      found = true
                    }
                  }
                }
                result.setBoolean(0, found)
                numOutputRows += 1
                return true
              }
              false
            }

            override def getRow: InternalRow = resultProj(joinRow(currentLeftRow, result))
          }.toScala

        case x =>
          throw new IllegalArgumentException(
            s"SortMergeJoin should not take $x as the JoinType")
      }
    }
  }





  /* ------------------------ *
   | Copied from ShuffledJoin |
   * ------------------------ */

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }






  /* ----------------------------- *
   | Copied from SortMergeJoinExec |
   * ----------------------------- */

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For inner join, orders of both sides keys should be kept.
    case _: InnerLike =>
      val leftKeyOrdering = getKeyOrdering(leftKeys, left.outputOrdering)
      val rightKeyOrdering = getKeyOrdering(rightKeys, right.outputOrdering)
      leftKeyOrdering.zip(rightKeyOrdering).map { case (lKey, rKey) =>
        // Also add expressions from right side sort order
        val sameOrderExpressions = ExpressionSet(lKey.sameOrderExpressions ++ rKey.children)
        SortOrder(lKey.child, Ascending, sameOrderExpressions.toSeq)
      }
    // For left and right outer joins, the output is ordered by the streamed input's join keys.
    case LeftOuter => getKeyOrdering(leftKeys, left.outputOrdering)
    case RightOuter => getKeyOrdering(rightKeys, right.outputOrdering)
    case LeftExistence(_) => getKeyOrdering(leftKeys, left.outputOrdering)
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  /**
   * The utility method to get output ordering for left or right side of the join.
   *
   * Returns the required ordering for left or right child if childOutputOrdering does not
   * satisfy the required ordering; otherwise, which means the child does not need to be sorted
   * again, returns the required ordering for this child with extra "sameOrderExpressions" from
   * the child's outputOrdering.
   */
  private def getKeyOrdering(keys: Seq[Expression], childOutputOrdering: Seq[SortOrder])
  : Seq[SortOrder] = {
    val requiredOrdering = requiredOrders(keys)
    if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
      keys.zip(childOutputOrdering).map { case (key, childOrder) =>
        val sameOrderExpressionsSet = ExpressionSet(childOrder.children) - key
        SortOrder(key, Ascending, sameOrderExpressionsSet.toSeq)
      }
    } else {
      requiredOrdering
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  private def createLeftKeyGenerator(): Projection =
    UnsafeProjection.create(leftKeys, left.output)

  private def createRightKeyGenerator(): Projection =
    UnsafeProjection.create(rightKeys, right.output)

  private def getSpillThreshold: Int = {
    sqlContext.conf.sortMergeJoinExecBufferSpillThreshold
  }

  private def getInMemoryThreshold: Int = {
    sqlContext.conf.sortMergeJoinExecBufferInMemoryThreshold
  }

  override def supportCodegen: Boolean = {
    // TODO: support codegen for InnerLike
    false
  }

  private def createJoinKey(
      ctx: CodegenContext,
      row: String,
      keys: Seq[Expression],
      input: Seq[Attribute]): Seq[ExprCode] = {
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    bindReferences(keys, input).map(_.genCode(ctx))
  }

  private def copyKeys(ctx: CodegenContext, vars: Seq[ExprCode]): Seq[ExprCode] = {
    vars.zipWithIndex.map { case (ev, i) =>
      ctx.addBufferedState(leftKeys(i).dataType, "value", ev.value)
    }
  }

  private def genComparison(ctx: CodegenContext, a: Seq[ExprCode], b: Seq[ExprCode]): String = {
    val comparisons = a.zip(b).zipWithIndex.map { case ((l, r), i) =>
      s"""
         |if (comp == 0) {
         |  comp = ${ctx.genComp(leftKeys(i).dataType, l.value, r.value)};
         |}
       """.stripMargin.trim
    }
    s"""
       |comp = 0;
       |${comparisons.mkString("\n")}
     """.stripMargin
  }

  /**
   * Generate a function to scan both left and right to find a match, returns the term for
   * matched one row from left side and buffered rows from right side.
   */
  private def genScanner(ctx: CodegenContext): (String, String) = {
    // Create class member for next row from both sides.
    // Inline mutable state since not many join operations in a task
    val leftRow = ctx.addMutableState("InternalRow", "leftRow", forceInline = true)
    val rightRow = ctx.addMutableState("InternalRow", "rightRow", forceInline = true)

    // Create variables for join keys from both sides.
    val leftKeyVars = createJoinKey(ctx, leftRow, leftKeys, left.output)
    val leftAnyNull = leftKeyVars.map(_.isNull).mkString(" || ")
    val rightKeyTmpVars = createJoinKey(ctx, rightRow, rightKeys, right.output)
    val rightAnyNull = rightKeyTmpVars.map(_.isNull).mkString(" || ")
    // Copy the right key as class members so they could be used in next function call.
    val rightKeyVars = copyKeys(ctx, rightKeyTmpVars)

    // A list to hold all matched rows from right side.
    val clsName = classOf[ExternalAppendOnlyUnsafeRowArray].getName

    val spillThreshold = getSpillThreshold
    val inMemoryThreshold = getInMemoryThreshold

    // Inline mutable state since not many join operations in a task
    val matches = ctx.addMutableState(clsName, "matches",
      v => s"$v = new $clsName($inMemoryThreshold, $spillThreshold);", forceInline = true)
    // Copy the left keys as class members so they could be used in next function call.
    val matchedKeyVars = copyKeys(ctx, leftKeyVars)

    ctx.addNewFunction("findNextInnerJoinRows",
      s"""
         |private boolean findNextInnerJoinRows(
         |    scala.collection.Iterator leftIter,
         |    scala.collection.Iterator rightIter) {
         |  $leftRow = null;
         |  int comp = 0;
         |  while ($leftRow == null) {
         |    if (!leftIter.hasNext()) return false;
         |    $leftRow = (InternalRow) leftIter.next();
         |    ${leftKeyVars.map(_.code).mkString("\n")}
         |    if ($leftAnyNull) {
         |      $leftRow = null;
         |      continue;
         |    }
         |    if (!$matches.isEmpty()) {
         |      ${genComparison(ctx, leftKeyVars, matchedKeyVars)}
         |      if (comp == 0) {
         |        return true;
         |      }
         |      $matches.clear();
         |    }
         |
         |    do {
         |      if ($rightRow == null) {
         |        if (!rightIter.hasNext()) {
         |          ${matchedKeyVars.map(_.code).mkString("\n")}
         |          return !$matches.isEmpty();
         |        }
         |        $rightRow = (InternalRow) rightIter.next();
         |        ${rightKeyTmpVars.map(_.code).mkString("\n")}
         |        if ($rightAnyNull) {
         |          $rightRow = null;
         |          continue;
         |        }
         |        ${rightKeyVars.map(_.code).mkString("\n")}
         |      }
         |      ${genComparison(ctx, leftKeyVars, rightKeyVars)}
         |      if (comp > 0) {
         |        $rightRow = null;
         |      } else if (comp < 0) {
         |        if (!$matches.isEmpty()) {
         |          ${matchedKeyVars.map(_.code).mkString("\n")}
         |          return true;
         |        }
         |        $leftRow = null;
         |      } else {
         |        $matches.add((UnsafeRow) $rightRow);
         |        $rightRow = null;
         |      }
         |    } while ($leftRow != null);
         |  }
         |  return false; // unreachable
         |}
       """.stripMargin, inlineToOuterClass = true)

    (leftRow, matches)
  }

  /**
   * Creates variables and declarations for left part of result row.
   *
   * In order to defer the access after condition and also only access once in the loop,
   * the variables should be declared separately from accessing the columns, we can't use the
   * codegen of BoundReference here.
   */
  private def createLeftVars(ctx: CodegenContext, leftRow: String): (Seq[ExprCode], Seq[String]) = {
    ctx.INPUT_ROW = leftRow
    left.output.zipWithIndex.map { case (a, i) =>
      val value = ctx.freshName("value")
      val valueCode = CodeGenerator.getValue(leftRow, a.dataType, i.toString)
      val javaType = CodeGenerator.javaType(a.dataType)
      val defaultValue = CodeGenerator.defaultValue(a.dataType)
      if (a.nullable) {
        val isNull = ctx.freshName("isNull")
        val code =
          code"""
             |$isNull = $leftRow.isNullAt($i);
             |$value = $isNull ? $defaultValue : ($valueCode);
           """.stripMargin
        val leftVarsDecl =
          s"""
             |boolean $isNull = false;
             |$javaType $value = $defaultValue;
           """.stripMargin
        (ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, a.dataType)),
          leftVarsDecl)
      } else {
        val code = code"$value = $valueCode;"
        val leftVarsDecl = s"""$javaType $value = $defaultValue;"""
        (ExprCode(code, FalseLiteral, JavaCode.variable(value, a.dataType)), leftVarsDecl)
      }
    }.unzip
  }

  /**
   * Creates the variables for right part of result row, using BoundReference, since the right
   * part are accessed inside the loop.
   */
  private def createRightVar(ctx: CodegenContext, rightRow: String): Seq[ExprCode] = {
    ctx.INPUT_ROW = rightRow
    right.output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable).genCode(ctx)
    }
  }

  /**
   * Splits variables based on whether it's used by condition or not, returns the code to create
   * these variables before the condition and after the condition.
   *
   * Only a few columns are used by condition, then we can skip the accessing of those columns
   * that are not used by condition also filtered out by condition.
   */
  private def splitVarsByCondition(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode]): (String, String) = {
    if (condition.isDefined) {
      val condRefs = condition.get.references
      val (used, notUsed) = attributes.zip(variables).partition{ case (a, ev) =>
        condRefs.contains(a)
      }
      val beforeCond = evaluateVariables(used.map(_._2))
      val afterCond = evaluateVariables(notUsed.map(_._2))
      (beforeCond, afterCond)
    } else {
      (evaluateVariables(variables), "")
    }
  }

  override def needCopyResult: Boolean = true

  override def doProduce(ctx: CodegenContext): String = {
    // Inline mutable state since not many join operations in a task
    val leftInput = ctx.addMutableState("scala.collection.Iterator", "leftInput",
      v => s"$v = inputs[0];", forceInline = true)
    val rightInput = ctx.addMutableState("scala.collection.Iterator", "rightInput",
      v => s"$v = inputs[1];", forceInline = true)

    val (leftRow, matches) = genScanner(ctx)

    // Create variables for row from both sides.
    val (leftVars, leftVarDecl) = createLeftVars(ctx, leftRow)
    val rightRow = ctx.freshName("rightRow")
    val rightVars = createRightVar(ctx, rightRow)

    val iterator = ctx.freshName("iterator")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val (beforeLoop, condCheck) = if (condition.isDefined) {
      // Split the code of creating variables based on whether it's used by condition or not.
      val loaded = ctx.freshName("loaded")
      val (leftBefore, leftAfter) = splitVarsByCondition(left.output, leftVars)
      val (rightBefore, rightAfter) = splitVarsByCondition(right.output, rightVars)
      // Generate code for condition
      ctx.currentVars = leftVars ++ rightVars
      val cond = BindReferences.bindReference(condition.get, output).genCode(ctx)
      // evaluate the columns those used by condition before loop
      val before = s"""
           |boolean $loaded = false;
           |$leftBefore
         """.stripMargin

      val checking = s"""
         |$rightBefore
         |${cond.code}
         |if (${cond.isNull} || !${cond.value}) continue;
         |if (!$loaded) {
         |  $loaded = true;
         |  $leftAfter
         |}
         |$rightAfter
     """.stripMargin
      (before, checking)
    } else {
      (evaluateVariables(leftVars), "")
    }

    val thisPlan = ctx.addReferenceObj("plan", this)
    val eagerCleanup = s"$thisPlan.cleanupResources();"

    s"""
       |while (findNextInnerJoinRows($leftInput, $rightInput)) {
       |  ${leftVarDecl.mkString("\n")}
       |  ${beforeLoop.trim}
       |  scala.collection.Iterator<UnsafeRow> $iterator = $matches.generateIterator();
       |  while ($iterator.hasNext()) {
       |    InternalRow $rightRow = (InternalRow) $iterator.next();
       |    ${condCheck.trim}
       |    $numOutput.add(1);
       |    ${consume(ctx, leftVars ++ rightVars)}
       |  }
       |  if (shouldStop()) return;
       |}
       |$eagerCleanup
     """.stripMargin
  }







  /* --------------------------------- *
   | Copied from BroadcastHashJoinExec |
   * --------------------------------- */

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.length == rightKeys.length &&
      leftKeys.map(_.dataType)
        .zip(rightKeys.map(_.dataType))
        .forall(types => types._1.sameType(types._2)),
      "Join keys from two sides should have same length and types")
    buildSide match {
      case BuildLeft => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  override lazy val outputPartitioning: Partitioning = {
    joinType match {
      // TODO: check broadcastHashJoinOutputPartitioningExpandLimit
      case _: InnerLike if sqlContext.conf.broadcastHashJoinOutputPartitioningExpandLimit > 0 =>
        streamedPlan.outputPartitioning match {
          case h: HashPartitioning => expandOutputPartitioning(h)
          case c: PartitioningCollection => expandOutputPartitioning(c)
          case other => other
        }
      case _ => streamedPlan.outputPartitioning
    }
  }

  // An one-to-many mapping from a streamed key to build keys.
  private lazy val streamedKeyToBuildKeyMapping = {
    val mapping = mutable.Map.empty[Expression, Seq[Expression]]
    streamedKeys.zip(buildKeys).foreach {
      case (streamedKey, buildKey) =>
        val key = streamedKey.canonicalized
        mapping.get(key) match {
          case Some(v) => mapping.put(key, v :+ buildKey)
          case None => mapping.put(key, Seq(buildKey))
        }
    }
    mapping.toMap
  }

  // Expands the given partitioning collection recursively.
  private def expandOutputPartitioning(
      partitioning: PartitioningCollection): PartitioningCollection = {
    PartitioningCollection(partitioning.partitionings.flatMap {
      case h: HashPartitioning => expandOutputPartitioning(h).partitionings
      case c: PartitioningCollection => Seq(expandOutputPartitioning(c))
      case other => Seq(other)
    })
  }

  // Expands the given hash partitioning by substituting streamed keys with build keys.
  // For example, if the expressions for the given partitioning are Seq("a", "b", "c")
  // where the streamed keys are Seq("b", "c") and the build keys are Seq("x", "y"),
  // the expanded partitioning will have the following expressions:
  // Seq("a", "b", "c"), Seq("a", "b", "y"), Seq("a", "x", "c"), Seq("a", "x", "y").
  // The expanded expressions are returned as PartitioningCollection.
  private def expandOutputPartitioning(partitioning: HashPartitioning): PartitioningCollection = {
    val maxNumCombinations = sqlContext.conf.broadcastHashJoinOutputPartitioningExpandLimit
    var currentNumCombinations = 0

    def generateExprCombinations(
        current: Seq[Expression],
        accumulated: Seq[Expression]): Seq[Seq[Expression]] = {
      if (currentNumCombinations >= maxNumCombinations) {
        Nil
      } else if (current.isEmpty) {
        currentNumCombinations += 1
        Seq(accumulated)
      } else {
        val buildKeysOpt = streamedKeyToBuildKeyMapping.get(current.head.canonicalized)
        generateExprCombinations(current.tail, accumulated :+ current.head) ++
          buildKeysOpt.map(_.flatMap(b => generateExprCombinations(current.tail, accumulated :+ b)))
            .getOrElse(Nil)
      }
    }

    PartitioningCollection(
      generateExprCombinations(partitioning.expressions, Nil)
        .map(HashPartitioning(_, partitioning.numPartitions)))
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }
}


object BroadcastSortJoinExec {

  def createInternalRowIterator(file: String): Iterator[InternalRow] = {
    val fis = new FileInputStream(file)
    val ois = new ObjectInputStream(fis)

    val iter = new Iterator[InternalRow]() {
      override def hasNext: Boolean =
        fis.available() > 0

      override def next(): InternalRow =
        ois.readObject().asInstanceOf[InternalRow]
    }

    CompletionIterator[InternalRow, Iterator[InternalRow]](iter, { ois.close(); fis.close() })
  }

  def createInternalRowIterator(files: Array[String]): Iterator[InternalRow] = {
    files.iterator.flatMap(createInternalRowIterator)
  }
}
