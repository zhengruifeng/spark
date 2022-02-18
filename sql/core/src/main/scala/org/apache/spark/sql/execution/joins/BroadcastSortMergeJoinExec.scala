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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.{PartitionRecombinedRDD, RDD}
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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.BitSet

/**
 * Performs a broadcast sort merge join of two child relations.
 */
case class BroadcastSortMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends ShuffledJoin {
  override def isSkewJoin: Boolean = false

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"))

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

  protected lazy val (_buildPlan, _streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  override def requiredChildDistribution: Seq[Distribution] = buildSide match {
    case BuildLeft =>
      AllTuples :: UnspecifiedDistribution :: Nil
    case BuildRight =>
      UnspecifiedDistribution :: AllTuples :: Nil
  }

  override def outputPartitioning: Partitioning = joinType match {
    case Inner | Cross | LeftOuter | RightOuter | LeftSemi | LeftAnti =>
      _streamedPlan.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"BroadcastSortMergeJoinExec should not take $x as the JoinType")
  }

  private lazy val recombinedBuildRDD = {
    val buildRDD = _buildPlan.execute().map(_.copy())
    assert(buildRDD.getNumPartitions == 1)
    buildRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val numPartitions = _streamedPlan.execute().getNumPartitions
    var numDuplicated = math.sqrt(numPartitions).ceil.toInt
    numDuplicated = math.max(numDuplicated, 2)
    numDuplicated = math.min(numDuplicated, 16)
    numDuplicated = math.min(numDuplicated, numPartitions)
    val duplicatedBuildRDD =
      new PartitionRecombinedRDD(buildRDD, Array.fill(numDuplicated)(Array(0)))
    duplicatedBuildRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

    new PartitionRecombinedRDD(duplicatedBuildRDD,
      Array.tabulate(numPartitions)(i => Array(i % numDuplicated)))
  }

  private lazy val (leftRDD, rightRDD) = buildSide match {
    case BuildLeft => (recombinedBuildRDD, right.execute())
    case BuildRight => (left.execute(), recombinedBuildRDD)
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
    conf.sortMergeJoinExecBufferSpillThreshold
  }

  // Flag to only buffer first matched row, to avoid buffering unnecessary rows.
  private val onlyBufferFirstMatchedRow = (joinType, condition) match {
    case (LeftExistence(_), None) => true
    case _ => false
  }

  private def getInMemoryThreshold: Int = {
    if (onlyBufferFirstMatchedRow) {
      1
    } else {
      conf.sortMergeJoinExecBufferInMemoryThreshold
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val spillSize = longMetric("spillSize")
    val spillThreshold = getSpillThreshold
    val inMemoryThreshold = getInMemoryThreshold
    leftRDD.zipPartitions(rightRDD) { (leftIter, rightIter) =>
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
              spillSize,
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
            spillSize,
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
            spillSize,
            cleanupResources
          )
          val leftNullRow = new GenericInternalRow(left.output.length)
          new RightOuterIterator(
            smjScanner, leftNullRow, boundCondition, resultProj, numOutputRows).toScala

        case FullOuter =>
          val leftNullRow = new GenericInternalRow(left.output.length)
          val rightNullRow = new GenericInternalRow(right.output.length)
          val smjScanner = new SortMergeFullOuterJoinScanner(
            leftKeyGenerator = createLeftKeyGenerator(),
            rightKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            leftIter = RowIterator.fromScala(leftIter),
            rightIter = RowIterator.fromScala(rightIter),
            boundCondition,
            leftNullRow,
            rightNullRow)

          new FullOuterIterator(
            smjScanner,
            resultProj,
            numOutputRows).toScala

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
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow
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
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow
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
              spillSize,
              cleanupResources,
              onlyBufferFirstMatchedRow
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

  private lazy val ((streamedPlan, streamedKeys), (bufferedPlan, bufferedKeys)) = joinType match {
    case _: InnerLike | LeftOuter | LeftExistence(_) =>
      ((left, leftKeys), (right, rightKeys))
    case RightOuter => ((right, rightKeys), (left, leftKeys))
    case x =>
      throw new IllegalArgumentException(
        s"BroadcastSortMergeJoinExec.streamedPlan/bufferedPlan should not take $x as the JoinType")
  }

  private lazy val streamedOutput = streamedPlan.output
  private lazy val bufferedOutput = bufferedPlan.output

  override def supportCodegen: Boolean = joinType match {
    case FullOuter => conf.getConf(SQLConf.ENABLE_FULL_OUTER_SORT_MERGE_JOIN_CODEGEN)
    case _: ExistenceJoin => conf.getConf(SQLConf.ENABLE_EXISTENCE_SORT_MERGE_JOIN_CODEGEN)
    case _ => true
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = joinType match {
    case _: InnerLike | LeftOuter | LeftExistence(_) => leftRDD :: rightRDD :: Nil
    case RightOuter => rightRDD :: leftRDD :: Nil
    case x =>
      throw new IllegalArgumentException(
        s"BroadcastSortMergeJoinExec.streamedPlan/bufferedPlan should not take $x as the JoinType")
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
   * Generate a function to scan both sides to find a match, returns:
   * 1. the function name
   * 2. the term for matched one row from streamed side
   * 3. the term for buffered rows from buffered side
   */
  private def genScanner(ctx: CodegenContext): (String, String, String) = {
    // Create class member for next row from both sides.
    // Inline mutable state since not many join operations in a task
    val streamedRow = ctx.addMutableState("InternalRow", "streamedRow", forceInline = true)
    val bufferedRow = ctx.addMutableState("InternalRow", "bufferedRow", forceInline = true)

    // Create variables for join keys from both sides.
    val streamedKeyVars = createJoinKey(ctx, streamedRow, streamedKeys, streamedOutput)
    val streamedAnyNull = streamedKeyVars.map(_.isNull).mkString(" || ")
    val bufferedKeyTmpVars = createJoinKey(ctx, bufferedRow, bufferedKeys, bufferedOutput)
    val bufferedAnyNull = bufferedKeyTmpVars.map(_.isNull).mkString(" || ")
    // Copy the buffered key as class members so they could be used in next function call.
    val bufferedKeyVars = copyKeys(ctx, bufferedKeyTmpVars)

    // A list to hold all matched rows from buffered side.
    val clsName = classOf[ExternalAppendOnlyUnsafeRowArray].getName

    val spillThreshold = getSpillThreshold
    val inMemoryThreshold = getInMemoryThreshold

    // Inline mutable state since not many join operations in a task
    val matches = ctx.addMutableState(clsName, "matches",
      v => s"$v = new $clsName($inMemoryThreshold, $spillThreshold);", forceInline = true)
    // Copy the streamed keys as class members so they could be used in next function call.
    val matchedKeyVars = copyKeys(ctx, streamedKeyVars)

    // Handle the case when streamed rows has any NULL keys.
    val handleStreamedAnyNull = joinType match {
      case _: InnerLike | LeftSemi =>
        // Skip streamed row.
        s"""
           |$streamedRow = null;
           |continue;
         """.stripMargin
      case LeftOuter | RightOuter | LeftAnti | ExistenceJoin(_) =>
        // Eagerly return streamed row. Only call `matches.clear()` when `matches.isEmpty()` is
        // false, to reduce unnecessary computation.
        s"""
           |if (!$matches.isEmpty()) {
           |  $matches.clear();
           |}
           |return false;
         """.stripMargin
      case x =>
        throw new IllegalArgumentException(
          s"SortMergeJoin.genScanner should not take $x as the JoinType")
    }

    // Handle the case when streamed keys has no match with buffered side.
    val handleStreamedWithoutMatch = joinType match {
      case _: InnerLike | LeftSemi =>
        // Skip streamed row.
        s"$streamedRow = null;"
      case LeftOuter | RightOuter | LeftAnti | ExistenceJoin(_) =>
        // Eagerly return with streamed row.
        "return false;"
      case x =>
        throw new IllegalArgumentException(
          s"SortMergeJoin.genScanner should not take $x as the JoinType")
    }

    val addRowToBuffer =
      if (onlyBufferFirstMatchedRow) {
        s"""
           |if ($matches.isEmpty()) {
           |  $matches.add((UnsafeRow) $bufferedRow);
           |}
         """.stripMargin
      } else {
        s"$matches.add((UnsafeRow) $bufferedRow);"
      }

    // Generate a function to scan both streamed and buffered sides to find a match.
    // Return whether a match is found.
    //
    // `streamedIter`: the iterator for streamed side.
    // `bufferedIter`: the iterator for buffered side.
    // `streamedRow`: the current row from streamed side.
    //                When `streamedIter` is empty, `streamedRow` is null.
    // `matches`: the rows from buffered side already matched with `streamedRow`.
    //            `matches` is buffered and reused for all `streamedRow`s having same join keys.
    //            If there is no match with `streamedRow`, `matches` is empty.
    // `bufferedRow`: the current matched row from buffered side.
    //
    // The function has the following step:
    //  - Step 1: Find the next `streamedRow` with non-null join keys.
    //            For `streamedRow` with null join keys (`handleStreamedAnyNull`):
    //            1. Inner and Left Semi join: skip the row. `matches` will be cleared later when
    //                                         hitting the next `streamedRow` with non-null join
    //                                         keys.
    //            2. Left/Right Outer, Left Anti and Existence join: clear the previous `matches`
    //                                                               if needed, keep the row, and
    //                                                               return false.
    //
    //  - Step 2: Find the `matches` from buffered side having same join keys with `streamedRow`.
    //            Clear `matches` if we hit a new `streamedRow`, as we need to find new matches.
    //            Use `bufferedRow` to iterate buffered side to put all matched rows into
    //            `matches` (`addRowToBuffer`). Return true when getting all matched rows.
    //            For `streamedRow` without `matches` (`handleStreamedWithoutMatch`):
    //            1. Inner and Left Semi join: skip the row.
    //            2. Left/Right Outer, Left Anti and Existence join: keep the row and return false
    //                                                               (with `matches` being empty).
    val findNextJoinRowsFuncName = ctx.freshName("findNextJoinRows")
    ctx.addNewFunction(findNextJoinRowsFuncName,
      s"""
         |private boolean $findNextJoinRowsFuncName(
         |    scala.collection.Iterator streamedIter,
         |    scala.collection.Iterator bufferedIter) {
         |  $streamedRow = null;
         |  int comp = 0;
         |  while ($streamedRow == null) {
         |    if (!streamedIter.hasNext()) return false;
         |    $streamedRow = (InternalRow) streamedIter.next();
         |    ${streamedKeyVars.map(_.code).mkString("\n")}
         |    if ($streamedAnyNull) {
         |      $handleStreamedAnyNull
         |    }
         |    if (!$matches.isEmpty()) {
         |      ${genComparison(ctx, streamedKeyVars, matchedKeyVars)}
         |      if (comp == 0) {
         |        return true;
         |      }
         |      $matches.clear();
         |    }
         |
         |    do {
         |      if ($bufferedRow == null) {
         |        if (!bufferedIter.hasNext()) {
         |          ${matchedKeyVars.map(_.code).mkString("\n")}
         |          return !$matches.isEmpty();
         |        }
         |        $bufferedRow = (InternalRow) bufferedIter.next();
         |        ${bufferedKeyTmpVars.map(_.code).mkString("\n")}
         |        if ($bufferedAnyNull) {
         |          $bufferedRow = null;
         |          continue;
         |        }
         |        ${bufferedKeyVars.map(_.code).mkString("\n")}
         |      }
         |      ${genComparison(ctx, streamedKeyVars, bufferedKeyVars)}
         |      if (comp > 0) {
         |        $bufferedRow = null;
         |      } else if (comp < 0) {
         |        if (!$matches.isEmpty()) {
         |          ${matchedKeyVars.map(_.code).mkString("\n")}
         |          return true;
         |        } else {
         |          $handleStreamedWithoutMatch
         |        }
         |      } else {
         |        $addRowToBuffer
         |        $bufferedRow = null;
         |      }
         |    } while ($streamedRow != null);
         |  }
         |  return false; // unreachable
         |}
       """.stripMargin, inlineToOuterClass = true)

    (findNextJoinRowsFuncName, streamedRow, matches)
  }

  /**
   * Creates variables and declarations for streamed part of result row.
   *
   * In order to defer the access after condition and also only access once in the loop,
   * the variables should be declared separately from accessing the columns, we can't use the
   * codegen of BoundReference here.
   */
  private def createStreamedVars(
      ctx: CodegenContext,
      streamedRow: String): (Seq[ExprCode], Seq[String]) = {
    ctx.INPUT_ROW = streamedRow
    streamedPlan.output.zipWithIndex.map { case (a, i) =>
      val value = ctx.freshName("value")
      val valueCode = CodeGenerator.getValue(streamedRow, a.dataType, i.toString)
      val javaType = CodeGenerator.javaType(a.dataType)
      val defaultValue = CodeGenerator.defaultValue(a.dataType)
      if (a.nullable) {
        val isNull = ctx.freshName("isNull")
        val code =
          code"""
             |$isNull = $streamedRow.isNullAt($i);
             |$value = $isNull ? $defaultValue : ($valueCode);
           """.stripMargin
        val streamedVarsDecl =
          s"""
             |boolean $isNull = false;
             |$javaType $value = $defaultValue;
           """.stripMargin
        (ExprCode(code, JavaCode.isNullVariable(isNull), JavaCode.variable(value, a.dataType)),
          streamedVarsDecl)
      } else {
        val code = code"$value = $valueCode;"
        val streamedVarsDecl = s"""$javaType $value = $defaultValue;"""
        (ExprCode(code, FalseLiteral, JavaCode.variable(value, a.dataType)), streamedVarsDecl)
      }
    }.unzip
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

  /**
   * This is called by generated Java class, should be public.
   */
  def getTaskContext(): TaskContext = {
    TaskContext.get()
  }

  override def doProduce(ctx: CodegenContext): String = {
    // Specialize `doProduce` code for full outer join, because full outer join needs to
    // buffer both sides of join.
    if (joinType == FullOuter) {
      return codegenFullOuter(ctx)
    }

    // Inline mutable state since not many join operations in a task
    val streamedInput = ctx.addMutableState("scala.collection.Iterator", "streamedInput",
      v => s"$v = inputs[0];", forceInline = true)
    val bufferedInput = ctx.addMutableState("scala.collection.Iterator", "bufferedInput",
      v => s"$v = inputs[1];", forceInline = true)

    val (findNextJoinRowsFuncName, streamedRow, matches) = genScanner(ctx)

    // Create variables for row from both sides.
    val (streamedVars, streamedVarDecl) = createStreamedVars(ctx, streamedRow)
    val bufferedRow = ctx.freshName("bufferedRow")
    val setDefaultValue = joinType == LeftOuter || joinType == RightOuter
    val bufferedVars = genOneSideJoinVars(ctx, bufferedRow, bufferedPlan, setDefaultValue)

    // Create variable name for Existence join.
    val existsVar = joinType match {
      case ExistenceJoin(_) => Some(ctx.freshName("exists"))
      case _ => None
    }

    val iterator = ctx.freshName("iterator")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val resultVars = joinType match {
      case _: InnerLike | LeftOuter =>
        streamedVars ++ bufferedVars
      case RightOuter =>
        bufferedVars ++ streamedVars
      case LeftSemi | LeftAnti =>
        streamedVars
      case ExistenceJoin(_) =>
        streamedVars ++ Seq(ExprCode.forNonNullValue(
          JavaCode.variable(existsVar.get, BooleanType)))
      case x =>
        throw new IllegalArgumentException(
          s"SortMergeJoin.doProduce should not take $x as the JoinType")
    }

    val (streamedBeforeLoop, condCheck, loadStreamed) = if (condition.isDefined) {
      // Split the code of creating variables based on whether it's used by condition or not.
      val loaded = ctx.freshName("loaded")
      val (streamedBefore, streamedAfter) = splitVarsByCondition(streamedOutput, streamedVars)
      val (bufferedBefore, bufferedAfter) = splitVarsByCondition(bufferedOutput, bufferedVars)
      // Generate code for condition
      ctx.currentVars = streamedVars ++ bufferedVars
      val cond = BindReferences.bindReference(
        condition.get, streamedPlan.output ++ bufferedPlan.output).genCode(ctx)
      // Evaluate the columns those used by condition before loop
      val before = joinType match {
        case LeftAnti =>
          // No need to initialize `loaded` variable for Left Anti join.
          streamedBefore.trim
        case _ =>
          s"""
             |boolean $loaded = false;
             |$streamedBefore
         """.stripMargin
      }

      val loadStreamedAfterCondition = joinType match {
        case LeftAnti =>
          // No need to evaluate columns not used by condition from streamed side, as for Left Anti
          // join, streamed row with match is not outputted.
          ""
        case _ =>
          s"""
             |if (!$loaded) {
             |  $loaded = true;
             |  $streamedAfter
             |}
         """.stripMargin
      }

      val loadBufferedAfterCondition = joinType match {
        case LeftExistence(_) =>
          // No need to evaluate columns not used by condition from buffered side
          ""
        case _ => bufferedAfter
      }

      val checking =
        s"""
           |$bufferedBefore
           |if ($bufferedRow != null) {
           |  ${cond.code}
           |  if (${cond.isNull} || !${cond.value}) {
           |    continue;
           |  }
           |}
           |$loadStreamedAfterCondition
           |$loadBufferedAfterCondition
         """.stripMargin
      (before, checking.trim, streamedAfter.trim)
    } else {
      (evaluateVariables(streamedVars), "", "")
    }

    val beforeLoop =
      s"""
         |${streamedVarDecl.mkString("\n")}
         |${streamedBeforeLoop.trim}
         |scala.collection.Iterator<UnsafeRow> $iterator = $matches.generateIterator();
       """.stripMargin
    val outputRow =
      s"""
         |$numOutput.add(1);
         |${consume(ctx, resultVars)}
       """.stripMargin
    val findNextJoinRows = s"$findNextJoinRowsFuncName($streamedInput, $bufferedInput)"
    val thisPlan = ctx.addReferenceObj("plan", this)
    val eagerCleanup = s"$thisPlan.cleanupResources();"

    val doJoin = joinType match {
      case _: InnerLike =>
        codegenInner(findNextJoinRows, beforeLoop, iterator, bufferedRow, condCheck, outputRow,
          eagerCleanup)
      case LeftOuter | RightOuter =>
        codegenOuter(streamedInput, findNextJoinRows, beforeLoop, iterator, bufferedRow, condCheck,
          ctx.freshName("hasOutputRow"), outputRow, eagerCleanup)
      case LeftSemi =>
        codegenSemi(findNextJoinRows, beforeLoop, iterator, bufferedRow, condCheck,
          ctx.freshName("hasOutputRow"), outputRow, eagerCleanup)
      case LeftAnti =>
        codegenAnti(streamedInput, findNextJoinRows, beforeLoop, iterator, bufferedRow, condCheck,
          loadStreamed, ctx.freshName("hasMatchedRow"), outputRow, eagerCleanup)
      case ExistenceJoin(_) =>
        codegenExistence(streamedInput, findNextJoinRows, beforeLoop, iterator, bufferedRow,
          condCheck, loadStreamed, existsVar.get, outputRow, eagerCleanup)
      case x =>
        throw new IllegalArgumentException(
          s"SortMergeJoin.doProduce should not take $x as the JoinType")
    }

    val initJoin = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initJoin")
    val addHookToRecordMetrics =
      s"""
         |$thisPlan.getTaskContext().addTaskCompletionListener(
         |  new org.apache.spark.util.TaskCompletionListener() {
         |    @Override
         |    public void onTaskCompletion(org.apache.spark.TaskContext context) {
         |      ${metricTerm(ctx, "spillSize")}.add($matches.spillSize());
         |    }
         |});
       """.stripMargin

    s"""
       |if (!$initJoin) {
       |  $initJoin = true;
       |  $addHookToRecordMetrics
       |}
       |$doJoin
     """.stripMargin
  }

  /**
   * Generates the code for Inner join.
   */
  private def codegenInner(
      findNextJoinRows: String,
      beforeLoop: String,
      matchIterator: String,
      bufferedRow: String,
      conditionCheck: String,
      outputRow: String,
      eagerCleanup: String): String = {
    s"""
       |while ($findNextJoinRows) {
       |  $beforeLoop
       |  while ($matchIterator.hasNext()) {
       |    InternalRow $bufferedRow = (InternalRow) $matchIterator.next();
       |    $conditionCheck
       |    $outputRow
       |  }
       |  if (shouldStop()) return;
       |}
       |$eagerCleanup
     """.stripMargin
  }

  /**
   * Generates the code for Left or Right Outer join.
   */
  private def codegenOuter(
      streamedInput: String,
      findNextJoinRows: String,
      beforeLoop: String,
      matchIterator: String,
      bufferedRow: String,
      conditionCheck: String,
      hasOutputRow: String,
      outputRow: String,
      eagerCleanup: String): String = {
    s"""
       |while ($streamedInput.hasNext()) {
       |  $findNextJoinRows;
       |  $beforeLoop
       |  boolean $hasOutputRow = false;
       |
       |  // the last iteration of this loop is to emit an empty row if there is no matched rows.
       |  while ($matchIterator.hasNext() || !$hasOutputRow) {
       |    InternalRow $bufferedRow = $matchIterator.hasNext() ?
       |      (InternalRow) $matchIterator.next() : null;
       |    $conditionCheck
       |    $hasOutputRow = true;
       |    $outputRow
       |  }
       |  if (shouldStop()) return;
       |}
       |$eagerCleanup
     """.stripMargin
  }

  /**
   * Generates the code for Left Semi join.
   */
  private def codegenSemi(
      findNextJoinRows: String,
      beforeLoop: String,
      matchIterator: String,
      bufferedRow: String,
      conditionCheck: String,
      hasOutputRow: String,
      outputRow: String,
      eagerCleanup: String): String = {
    s"""
       |while ($findNextJoinRows) {
       |  $beforeLoop
       |  boolean $hasOutputRow = false;
       |
       |  while (!$hasOutputRow && $matchIterator.hasNext()) {
       |    InternalRow $bufferedRow = (InternalRow) $matchIterator.next();
       |    $conditionCheck
       |    $hasOutputRow = true;
       |    $outputRow
       |  }
       |  if (shouldStop()) return;
       |}
       |$eagerCleanup
     """.stripMargin
  }

  /**
   * Generates the code for Left Anti join.
   */
  private def codegenAnti(
      streamedInput: String,
      findNextJoinRows: String,
      beforeLoop: String,
      matchIterator: String,
      bufferedRow: String,
      conditionCheck: String,
      loadStreamed: String,
      hasMatchedRow: String,
      outputRow: String,
      eagerCleanup: String): String = {
    s"""
       |while ($streamedInput.hasNext()) {
       |  $findNextJoinRows;
       |  $beforeLoop
       |  boolean $hasMatchedRow = false;
       |
       |  while (!$hasMatchedRow && $matchIterator.hasNext()) {
       |    InternalRow $bufferedRow = (InternalRow) $matchIterator.next();
       |    $conditionCheck
       |    $hasMatchedRow = true;
       |  }
       |
       |  if (!$hasMatchedRow) {
       |    // load all values of streamed row, because the values not in join condition are not
       |    // loaded yet.
       |    $loadStreamed
       |    $outputRow
       |  }
       |  if (shouldStop()) return;
       |}
       |$eagerCleanup
     """.stripMargin
  }

  /**
   * Generates the code for Existence join.
   */
  private def codegenExistence(
      streamedInput: String,
      findNextJoinRows: String,
      beforeLoop: String,
      matchIterator: String,
      bufferedRow: String,
      conditionCheck: String,
      loadStreamed: String,
      exists: String,
      outputRow: String,
      eagerCleanup: String): String = {
    s"""
       |while ($streamedInput.hasNext()) {
       |  $findNextJoinRows;
       |  $beforeLoop
       |  boolean $exists = false;
       |
       |  while (!$exists && $matchIterator.hasNext()) {
       |    InternalRow $bufferedRow = (InternalRow) $matchIterator.next();
       |    $conditionCheck
       |    $exists = true;
       |  }
       |
       |  if (!$exists) {
       |    // load all values of streamed row, because the values not in join condition are not
       |    // loaded yet.
       |    $loadStreamed
       |  }
       |  $outputRow
       |
       |  if (shouldStop()) return;
       |}
       |$eagerCleanup
     """.stripMargin
  }

  /**
   * Generates the code for Full Outer join.
   */
  private def codegenFullOuter(ctx: CodegenContext): String = {
    // Inline mutable state since not many join operations in a task.
    // Create class member for input iterator from both sides.
    val leftInput = ctx.addMutableState("scala.collection.Iterator", "leftInput",
      v => s"$v = inputs[0];", forceInline = true)
    val rightInput = ctx.addMutableState("scala.collection.Iterator", "rightInput",
      v => s"$v = inputs[1];", forceInline = true)

    // Create class member for next input row from both sides.
    val leftInputRow = ctx.addMutableState("InternalRow", "leftInputRow", forceInline = true)
    val rightInputRow = ctx.addMutableState("InternalRow", "rightInputRow", forceInline = true)

    // Create variables for join keys from both sides.
    val leftKeyVars = createJoinKey(ctx, leftInputRow, leftKeys, left.output)
    val leftAnyNull = leftKeyVars.map(_.isNull).mkString(" || ")
    val rightKeyVars = createJoinKey(ctx, rightInputRow, rightKeys, right.output)
    val rightAnyNull = rightKeyVars.map(_.isNull).mkString(" || ")
    val matchedKeyVars = copyKeys(ctx, leftKeyVars)
    val leftMatchedKeyVars = createJoinKey(ctx, leftInputRow, leftKeys, left.output)
    val rightMatchedKeyVars = createJoinKey(ctx, rightInputRow, rightKeys, right.output)

    // Create class member for next output row from both sides.
    val leftOutputRow = ctx.addMutableState("InternalRow", "leftOutputRow", forceInline = true)
    val rightOutputRow = ctx.addMutableState("InternalRow", "rightOutputRow", forceInline = true)

    // Create class member for buffers of rows with same join keys from both sides.
    val bufferClsName = "java.util.ArrayList<InternalRow>"
    val leftBuffer = ctx.addMutableState(bufferClsName, "leftBuffer",
      v => s"$v = new $bufferClsName();", forceInline = true)
    val rightBuffer = ctx.addMutableState(bufferClsName, "rightBuffer",
      v => s"$v = new $bufferClsName();", forceInline = true)
    val matchedClsName = classOf[BitSet].getName
    val leftMatched = ctx.addMutableState(matchedClsName, "leftMatched",
      v => s"$v = new $matchedClsName(1);", forceInline = true)
    val rightMatched = ctx.addMutableState(matchedClsName, "rightMatched",
      v => s"$v = new $matchedClsName(1);", forceInline = true)
    val leftIndex = ctx.freshName("leftIndex")
    val rightIndex = ctx.freshName("rightIndex")

    // Generate code for join condition
    val leftResultVars = genOneSideJoinVars(
      ctx, leftOutputRow, left, setDefaultValue = true)
    val rightResultVars = genOneSideJoinVars(
      ctx, rightOutputRow, right, setDefaultValue = true)
    val resultVars = leftResultVars ++ rightResultVars
    val (_, conditionCheck, _) =
      getJoinCondition(ctx, leftResultVars, left, right, Some(rightOutputRow))

    // Generate code for result output in separate function, as we need to output result from
    // multiple places in join code.
    val consumeFullOuterJoinRow = ctx.freshName("consumeFullOuterJoinRow")
    ctx.addNewFunction(consumeFullOuterJoinRow,
      s"""
         |private void $consumeFullOuterJoinRow() throws java.io.IOException {
         |  ${metricTerm(ctx, "numOutputRows")}.add(1);
         |  ${consume(ctx, resultVars)}
         |}
       """.stripMargin)

    // Handle the case when input row has no match.
    val outputLeftNoMatch =
      s"""
         |$leftOutputRow = $leftInputRow;
         |$rightOutputRow = null;
         |$leftInputRow = null;
         |$consumeFullOuterJoinRow();
       """.stripMargin
    val outputRightNoMatch =
      s"""
         |$rightOutputRow = $rightInputRow;
         |$leftOutputRow = null;
         |$rightInputRow = null;
         |$consumeFullOuterJoinRow();
       """.stripMargin

    // Generate a function to scan both sides to find rows with matched join keys.
    // The matched rows from both sides are copied in buffers separately. This function assumes
    // either non-empty `leftIter` and `rightIter`, or non-null `leftInputRow` and `rightInputRow`.
    //
    // The function has the following steps:
    //  - Step 1: Find the next `leftInputRow` and `rightInputRow` with non-null join keys.
    //            Output row with null join keys (`outputLeftNoMatch` and `outputRightNoMatch`).
    //
    //  - Step 2: Compare and find next same join keys from between `leftInputRow` and
    //            `rightInputRow`.
    //            Output row with smaller join keys (`outputLeftNoMatch` and `outputRightNoMatch`).
    //
    //  - Step 3: Buffer rows with same join keys from both sides into `leftBuffer` and
    //            `rightBuffer`. Reset bit sets for both buffers accordingly (`leftMatched` and
    //            `rightMatched`).
    val findNextJoinRowsFuncName = ctx.freshName("findNextJoinRows")
    ctx.addNewFunction(findNextJoinRowsFuncName,
      s"""
         |private void $findNextJoinRowsFuncName(
         |    scala.collection.Iterator leftIter,
         |    scala.collection.Iterator rightIter) throws java.io.IOException {
         |  int comp = 0;
         |  $leftBuffer.clear();
         |  $rightBuffer.clear();
         |
         |  if ($leftInputRow == null) {
         |    $leftInputRow = (InternalRow) leftIter.next();
         |  }
         |  if ($rightInputRow == null) {
         |    $rightInputRow = (InternalRow) rightIter.next();
         |  }
         |
         |  ${leftKeyVars.map(_.code).mkString("\n")}
         |  if ($leftAnyNull) {
         |    // The left row join key is null, join it with null row
         |    $outputLeftNoMatch
         |    return;
         |  }
         |
         |  ${rightKeyVars.map(_.code).mkString("\n")}
         |  if ($rightAnyNull) {
         |    // The right row join key is null, join it with null row
         |    $outputRightNoMatch
         |    return;
         |  }
         |
         |  ${genComparison(ctx, leftKeyVars, rightKeyVars)}
         |  if (comp < 0) {
         |    // The left row join key is smaller, join it with null row
         |    $outputLeftNoMatch
         |    return;
         |  } else if (comp > 0) {
         |    // The right row join key is smaller, join it with null row
         |    $outputRightNoMatch
         |    return;
         |  }
         |
         |  ${matchedKeyVars.map(_.code).mkString("\n")}
         |  $leftBuffer.add($leftInputRow.copy());
         |  $rightBuffer.add($rightInputRow.copy());
         |  $leftInputRow = null;
         |  $rightInputRow = null;
         |
         |  // Buffer rows from both sides with same join key
         |  while (leftIter.hasNext()) {
         |    $leftInputRow = (InternalRow) leftIter.next();
         |    ${leftMatchedKeyVars.map(_.code).mkString("\n")}
         |    ${genComparison(ctx, leftMatchedKeyVars, matchedKeyVars)}
         |    if (comp == 0) {
         |
         |      $leftBuffer.add($leftInputRow.copy());
         |      $leftInputRow = null;
         |    } else {
         |      break;
         |    }
         |  }
         |  while (rightIter.hasNext()) {
         |    $rightInputRow = (InternalRow) rightIter.next();
         |    ${rightMatchedKeyVars.map(_.code).mkString("\n")}
         |    ${genComparison(ctx, rightMatchedKeyVars, matchedKeyVars)}
         |    if (comp == 0) {
         |      $rightBuffer.add($rightInputRow.copy());
         |      $rightInputRow = null;
         |    } else {
         |      break;
         |    }
         |  }
         |
         |  // Reset bit sets of buffers accordingly
         |  if ($leftBuffer.size() <= $leftMatched.capacity()) {
         |    $leftMatched.clearUntil($leftBuffer.size());
         |  } else {
         |    $leftMatched = new $matchedClsName($leftBuffer.size());
         |  }
         |  if ($rightBuffer.size() <= $rightMatched.capacity()) {
         |    $rightMatched.clearUntil($rightBuffer.size());
         |  } else {
         |    $rightMatched = new $matchedClsName($rightBuffer.size());
         |  }
         |}
       """.stripMargin)

    // Scan the left and right buffers to find all matched rows.
    val matchRowsInBuffer =
      s"""
         |int $leftIndex;
         |int $rightIndex;
         |
         |for ($leftIndex = 0; $leftIndex < $leftBuffer.size(); $leftIndex++) {
         |  $leftOutputRow = (InternalRow) $leftBuffer.get($leftIndex);
         |  for ($rightIndex = 0; $rightIndex < $rightBuffer.size(); $rightIndex++) {
         |    $rightOutputRow = (InternalRow) $rightBuffer.get($rightIndex);
         |    $conditionCheck {
         |      $consumeFullOuterJoinRow();
         |      $leftMatched.set($leftIndex);
         |      $rightMatched.set($rightIndex);
         |    }
         |  }
         |
         |  if (!$leftMatched.get($leftIndex)) {
         |
         |    $rightOutputRow = null;
         |    $consumeFullOuterJoinRow();
         |  }
         |}
         |
         |$leftOutputRow = null;
         |for ($rightIndex = 0; $rightIndex < $rightBuffer.size(); $rightIndex++) {
         |  if (!$rightMatched.get($rightIndex)) {
         |    // The right row has never matched any left row, join it with null row
         |    $rightOutputRow = (InternalRow) $rightBuffer.get($rightIndex);
         |    $consumeFullOuterJoinRow();
         |  }
         |}
       """.stripMargin

    s"""
       |while (($leftInputRow != null || $leftInput.hasNext()) &&
       |  ($rightInputRow != null || $rightInput.hasNext())) {
       |  $findNextJoinRowsFuncName($leftInput, $rightInput);
       |  $matchRowsInBuffer
       |  if (shouldStop()) return;
       |}
       |
       |// The right iterator has no more rows, join left row with null
       |while ($leftInputRow != null || $leftInput.hasNext()) {
       |  if ($leftInputRow == null) {
       |    $leftInputRow = (InternalRow) $leftInput.next();
       |  }
       |  $outputLeftNoMatch
       |  if (shouldStop()) return;
       |}
       |
       |// The left iterator has no more rows, join right row with null
       |while ($rightInputRow != null || $rightInput.hasNext()) {
       |  if ($rightInputRow == null) {
       |    $rightInputRow = (InternalRow) $rightInput.next();
       |  }
       |  $outputRightNoMatch
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }


  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): BroadcastSortMergeJoinExec =
    copy(left = newLeft, right = newRight)
}

