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

package org.apache.spark.sql.connect.planner

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.protobuf.{Any => ProtoAny}
import io.grpc.stub.StreamObserver

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.connect.proto
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.{expressions, AliasIdentifier}
import org.apache.spark.sql.catalyst.analysis.{ParameterizedQuery, UnresolvedAlias, UnresolvedAttribute, UnresolvedDeserializer, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter, UsingJoin}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{CollectMetrics, Deduplicate, DeduplicateWithinWatermark, DeserializeToObject, Except, Intersect, LocalRelation, LogicalPlan, MapPartitions, Project, Sample, SerializeFromObject, Sort, SubqueryAlias, TypedFilter, Union, Unpivot, UnresolvedHint}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connect.artifact.SparkConnectArtifactManager
import org.apache.spark.sql.connect.common.{DataTypeProtoConverter, InvalidPlanInput, LiteralValueProtoConverter, UdfPacket}
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.connect.service.{SparkConnectCatalogHandler, SparkConnectCommandHandler}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

final case class InvalidCommandInput(
    private val message: String = "",
    private val cause: Throwable = null)
    extends Exception(message, cause)

class SparkConnectPlanner(val session: SparkSession) {
  private[connect] val exprTransformer = new ExpressionTransformer(this)

  // The root of the query plan is a relation and we apply the transformations to it.
  def transformRelation(rel: proto.Relation): LogicalPlan = {
    val plan = rel.getRelTypeCase match {
      // DataFrame API
      case proto.Relation.RelTypeCase.SHOW_STRING => transformShowString(rel.getShowString)
      case proto.Relation.RelTypeCase.HTML_STRING => transformHtmlString(rel.getHtmlString)
      case proto.Relation.RelTypeCase.READ => transformReadRel(rel.getRead)
      case proto.Relation.RelTypeCase.PROJECT => transformProject(rel.getProject)
      case proto.Relation.RelTypeCase.FILTER => transformFilter(rel.getFilter)
      case proto.Relation.RelTypeCase.LIMIT => transformLimit(rel.getLimit)
      case proto.Relation.RelTypeCase.OFFSET => transformOffset(rel.getOffset)
      case proto.Relation.RelTypeCase.TAIL => transformTail(rel.getTail)
      case proto.Relation.RelTypeCase.JOIN => transformJoin(rel.getJoin)
      case proto.Relation.RelTypeCase.DEDUPLICATE => transformDeduplicate(rel.getDeduplicate)
      case proto.Relation.RelTypeCase.SET_OP => transformSetOperation(rel.getSetOp)
      case proto.Relation.RelTypeCase.SORT => transformSort(rel.getSort)
      case proto.Relation.RelTypeCase.DROP => transformDrop(rel.getDrop)
      case proto.Relation.RelTypeCase.AGGREGATE => transformAggregate(rel.getAggregate)
      case proto.Relation.RelTypeCase.SQL => transformSql(rel.getSql)
      case proto.Relation.RelTypeCase.LOCAL_RELATION =>
        transformLocalRelation(rel.getLocalRelation)
      case proto.Relation.RelTypeCase.SAMPLE => transformSample(rel.getSample)
      case proto.Relation.RelTypeCase.RANGE => transformRange(rel.getRange)
      case proto.Relation.RelTypeCase.SUBQUERY_ALIAS =>
        transformSubqueryAlias(rel.getSubqueryAlias)
      case proto.Relation.RelTypeCase.REPARTITION => transformRepartition(rel.getRepartition)
      case proto.Relation.RelTypeCase.FILL_NA => transformNAFill(rel.getFillNa)
      case proto.Relation.RelTypeCase.DROP_NA => transformNADrop(rel.getDropNa)
      case proto.Relation.RelTypeCase.REPLACE => transformReplace(rel.getReplace)
      case proto.Relation.RelTypeCase.SUMMARY => transformStatSummary(rel.getSummary)
      case proto.Relation.RelTypeCase.DESCRIBE => transformStatDescribe(rel.getDescribe)
      case proto.Relation.RelTypeCase.COV => transformStatCov(rel.getCov)
      case proto.Relation.RelTypeCase.CORR => transformStatCorr(rel.getCorr)
      case proto.Relation.RelTypeCase.APPROX_QUANTILE =>
        transformStatApproxQuantile(rel.getApproxQuantile)
      case proto.Relation.RelTypeCase.CROSSTAB =>
        transformStatCrosstab(rel.getCrosstab)
      case proto.Relation.RelTypeCase.FREQ_ITEMS => transformStatFreqItems(rel.getFreqItems)
      case proto.Relation.RelTypeCase.SAMPLE_BY =>
        transformStatSampleBy(rel.getSampleBy)
      case proto.Relation.RelTypeCase.TO_SCHEMA => transformToSchema(rel.getToSchema)
      case proto.Relation.RelTypeCase.TO_DF =>
        transformToDF(rel.getToDf)
      case proto.Relation.RelTypeCase.WITH_COLUMNS_RENAMED =>
        transformWithColumnsRenamed(rel.getWithColumnsRenamed)
      case proto.Relation.RelTypeCase.WITH_COLUMNS => transformWithColumns(rel.getWithColumns)
      case proto.Relation.RelTypeCase.WITH_WATERMARK =>
        transformWithWatermark(rel.getWithWatermark)
      case proto.Relation.RelTypeCase.HINT => transformHint(rel.getHint)
      case proto.Relation.RelTypeCase.UNPIVOT => transformUnpivot(rel.getUnpivot)
      case proto.Relation.RelTypeCase.REPARTITION_BY_EXPRESSION =>
        transformRepartitionByExpression(rel.getRepartitionByExpression)
      case proto.Relation.RelTypeCase.MAP_PARTITIONS =>
        transformMapPartitions(rel.getMapPartitions)
      case proto.Relation.RelTypeCase.GROUP_MAP =>
        transformGroupMap(rel.getGroupMap)
      case proto.Relation.RelTypeCase.CO_GROUP_MAP =>
        transformCoGroupMap(rel.getCoGroupMap)
      case proto.Relation.RelTypeCase.APPLY_IN_PANDAS_WITH_STATE =>
        transformApplyInPandasWithState(rel.getApplyInPandasWithState)
      case proto.Relation.RelTypeCase.COLLECT_METRICS =>
        transformCollectMetrics(rel.getCollectMetrics)
      case proto.Relation.RelTypeCase.PARSE => transformParse(rel.getParse)
      case proto.Relation.RelTypeCase.RELTYPE_NOT_SET =>
        throw new IndexOutOfBoundsException("Expected Relation to be set, but is empty.")

      // Catalog API (internal-only)
      case proto.Relation.RelTypeCase.CATALOG =>
        // Normally, the Catalog requests are already processed in
        // SparkConnectStreamHandler#handlePlan, here still match 'CATALOG'
        // for the usage in extensions.
        new SparkConnectCatalogHandler(this).handle(rel.getCatalog)

      // Handle plugins for Spark Connect Relation types.
      case proto.Relation.RelTypeCase.EXTENSION =>
        transformRelationPlugin(rel.getExtension)
      case _ => throw InvalidPlanInput(s"${rel.getUnknown} not supported.")
    }

    if (rel.hasCommon && rel.getCommon.hasPlanId) {
      plan.setTagValue(LogicalPlan.PLAN_ID_TAG, rel.getCommon.getPlanId)
    }
    plan
  }

  private def transformRelationPlugin(extension: ProtoAny): LogicalPlan = {
    SparkConnectPluginRegistry.relationRegistry
      // Lazily traverse the collection.
      .view
      // Apply the transformation.
      .map(p => p.transform(extension, this))
      // Find the first non-empty transformation or throw.
      .find(_.nonEmpty)
      .flatten
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
  }

  private def transformShowString(rel: proto.ShowString): LogicalPlan = {
    val showString = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .showString(rel.getNumRows, rel.getTruncate, rel.getVertical)
    LocalRelation.fromProduct(
      output = AttributeReference("show_string", StringType, false)() :: Nil,
      data = Tuple1.apply(showString) :: Nil)
  }

  private def transformHtmlString(rel: proto.HtmlString): LogicalPlan = {
    val htmlString = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .htmlString(rel.getNumRows, rel.getTruncate)
    LocalRelation.fromProduct(
      output = AttributeReference("html_string", StringType, false)() :: Nil,
      data = Tuple1.apply(htmlString) :: Nil)
  }

  private def transformSql(sql: proto.SQL): LogicalPlan = {
    val args = sql.getArgsMap
    val parser = session.sessionState.sqlParser
    val parsedPlan = parser.parsePlan(sql.getQuery)
    if (!args.isEmpty) {
      ParameterizedQuery(
        parsedPlan,
        args.asScala
          .mapValues(exprTransformer.transformLiteral)
          .toMap)
    } else {
      parsedPlan
    }
  }

  private def transformSubqueryAlias(alias: proto.SubqueryAlias): LogicalPlan = {
    val aliasIdentifier =
      if (alias.getQualifierCount > 0) {
        AliasIdentifier.apply(alias.getAlias, alias.getQualifierList.asScala.toSeq)
      } else {
        AliasIdentifier.apply(alias.getAlias)
      }
    SubqueryAlias(aliasIdentifier, transformRelation(alias.getInput))
  }

  /**
   * All fields of [[proto.Sample]] are optional. However, given those are proto primitive types,
   * we cannot differentiate if the field is not or set when the field's value equals to the type
   * default value. In the future if this ever become a problem, one solution could be that to
   * wrap such fields into proto messages.
   */
  private def transformSample(rel: proto.Sample): LogicalPlan = {
    val plan = if (rel.getDeterministicOrder) {
      val input = Dataset.ofRows(session, transformRelation(rel.getInput))

      // It is possible that the underlying dataframe doesn't guarantee the ordering of rows in its
      // constituent partitions each time a split is materialized which could result in
      // overlapping splits. To prevent this, we explicitly sort each input partition to make the
      // ordering deterministic. Note that MapTypes cannot be sorted and are explicitly pruned out
      // from the sort order.
      val sortOrder = input.logicalPlan.output
        .filter(attr => RowOrdering.isOrderable(attr.dataType))
        .map(SortOrder(_, Ascending))
      if (sortOrder.nonEmpty) {
        Sort(sortOrder, global = false, input.logicalPlan)
      } else {
        input.cache()
        input.logicalPlan
      }
    } else {
      transformRelation(rel.getInput)
    }

    Sample(
      rel.getLowerBound,
      rel.getUpperBound,
      rel.getWithReplacement,
      if (rel.hasSeed) rel.getSeed else Utils.random.nextLong,
      plan)
  }

  private def transformRepartition(rel: proto.Repartition): LogicalPlan = {
    logical.Repartition(rel.getNumPartitions, rel.getShuffle, transformRelation(rel.getInput))
  }

  private def transformRange(rel: proto.Range): LogicalPlan = {
    val start = rel.getStart
    val end = rel.getEnd
    val step = rel.getStep
    val numPartitions = if (rel.hasNumPartitions) {
      rel.getNumPartitions
    } else {
      session.leafNodeDefaultParallelism
    }
    logical.Range(start, end, step, numPartitions)
  }

  private def transformNAFill(rel: proto.NAFill): LogicalPlan = {
    if (rel.getValuesCount == 0) {
      throw InvalidPlanInput(s"values must contains at least 1 item!")
    }
    if (rel.getValuesCount > 1 && rel.getValuesCount != rel.getColsCount) {
      throw InvalidPlanInput(
        s"When values contains more than 1 items, " +
          s"values and cols should have the same length!")
    }

    val dataset = Dataset.ofRows(session, transformRelation(rel.getInput))

    val cols = rel.getColsList.asScala.toArray
    val values = rel.getValuesList.asScala.toArray
    if (values.length == 1) {
      val value = LiteralValueProtoConverter.toCatalystValue(values.head)
      val columns = if (cols.nonEmpty) Some(cols.toSeq) else None
      dataset.na.fillValue(value, columns).logicalPlan
    } else {
      val valueMap = mutable.Map.empty[String, Any]
      cols.zip(values).foreach { case (col, value) =>
        valueMap.update(col, LiteralValueProtoConverter.toCatalystValue(value))
      }
      dataset.na.fill(valueMap = valueMap.toMap).logicalPlan
    }
  }

  private def transformNADrop(rel: proto.NADrop): LogicalPlan = {
    val dataset = Dataset.ofRows(session, transformRelation(rel.getInput))

    val cols = rel.getColsList.asScala.toArray

    (cols.nonEmpty, rel.hasMinNonNulls) match {
      case (true, true) =>
        dataset.na.drop(minNonNulls = rel.getMinNonNulls, cols = cols).logicalPlan
      case (true, false) =>
        dataset.na.drop(cols = cols).logicalPlan
      case (false, true) =>
        dataset.na.drop(minNonNulls = rel.getMinNonNulls).logicalPlan
      case (false, false) =>
        dataset.na.drop().logicalPlan
    }
  }

  private def transformReplace(rel: proto.NAReplace): LogicalPlan = {
    val replacement = mutable.Map.empty[Any, Any]
    rel.getReplacementsList.asScala.foreach { replace =>
      replacement.update(
        LiteralValueProtoConverter.toCatalystValue(replace.getOldValue),
        LiteralValueProtoConverter.toCatalystValue(replace.getNewValue))
    }

    if (rel.getColsCount == 0) {
      Dataset
        .ofRows(session, transformRelation(rel.getInput))
        .na
        .replace("*", replacement.toMap)
        .logicalPlan
    } else {
      Dataset
        .ofRows(session, transformRelation(rel.getInput))
        .na
        .replace(rel.getColsList.asScala.toSeq, replacement.toMap)
        .logicalPlan
    }
  }

  private def transformStatSummary(rel: proto.StatSummary): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .summary(rel.getStatisticsList.asScala.toSeq: _*)
      .logicalPlan
  }

  private def transformStatDescribe(rel: proto.StatDescribe): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .describe(rel.getColsList.asScala.toSeq: _*)
      .logicalPlan
  }

  private def transformStatCov(rel: proto.StatCov): LogicalPlan = {
    val cov = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .cov(rel.getCol1, rel.getCol2)
    LocalRelation.fromProduct(
      output = AttributeReference("cov", DoubleType, false)() :: Nil,
      data = Tuple1.apply(cov) :: Nil)
  }

  private def transformStatCorr(rel: proto.StatCorr): LogicalPlan = {
    val df = Dataset.ofRows(session, transformRelation(rel.getInput))
    val corr = if (rel.hasMethod) {
      df.stat.corr(rel.getCol1, rel.getCol2, rel.getMethod)
    } else {
      df.stat.corr(rel.getCol1, rel.getCol2)
    }

    LocalRelation.fromProduct(
      output = AttributeReference("corr", DoubleType, false)() :: Nil,
      data = Tuple1.apply(corr) :: Nil)
  }

  private def transformStatApproxQuantile(rel: proto.StatApproxQuantile): LogicalPlan = {
    val cols = rel.getColsList.asScala.toArray
    val probabilities = rel.getProbabilitiesList.asScala.map(_.doubleValue()).toArray
    val approxQuantile = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .approxQuantile(cols, probabilities, rel.getRelativeError)
    LocalRelation.fromProduct(
      output =
        AttributeReference("approx_quantile", ArrayType(ArrayType(DoubleType)), false)() :: Nil,
      data = Tuple1.apply(approxQuantile) :: Nil)
  }

  private def transformStatCrosstab(rel: proto.StatCrosstab): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .crosstab(rel.getCol1, rel.getCol2)
      .logicalPlan
  }

  private def transformStatFreqItems(rel: proto.StatFreqItems): LogicalPlan = {
    val cols = rel.getColsList.asScala.toSeq
    val df = Dataset.ofRows(session, transformRelation(rel.getInput))
    if (rel.hasSupport) {
      df.stat.freqItems(cols, rel.getSupport).logicalPlan
    } else {
      df.stat.freqItems(cols).logicalPlan
    }
  }

  private def transformStatSampleBy(rel: proto.StatSampleBy): LogicalPlan = {
    val fractions = rel.getFractionsList.asScala.toSeq.map { protoFraction =>
      val stratum = exprTransformer.transformLiteral(protoFraction.getStratum) match {
        case Literal(s, StringType) if s != null => s.toString
        case literal => literal.value
      }
      (stratum, protoFraction.getFraction)
    }

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .stat
      .sampleBy(
        col = Column(transformExpression(rel.getCol)),
        fractions = fractions.toMap,
        seed = if (rel.hasSeed) rel.getSeed else Utils.random.nextLong)
      .logicalPlan
  }

  private def transformToSchema(rel: proto.ToSchema): LogicalPlan = {
    val schema = transformDataType(rel.getSchema)
    assert(schema.isInstanceOf[StructType])

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .to(schema.asInstanceOf[StructType])
      .logicalPlan
  }

  private def transformToDF(rel: proto.ToDF): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .toDF(rel.getColumnNamesList.asScala.toSeq: _*)
      .logicalPlan
  }

  private def transformMapPartitions(rel: proto.MapPartitions): LogicalPlan = {
    val baseRel = transformRelation(rel.getInput)
    val commonUdf = rel.getFunc
    commonUdf.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
        transformTypedMapPartitions(commonUdf, baseRel)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        val pythonUdf = exprTransformer.transformPythonUDF(commonUdf)
        val isBarrier = if (rel.hasIsBarrier) rel.getIsBarrier else false
        pythonUdf.evalType match {
          case PythonEvalType.SQL_MAP_PANDAS_ITER_UDF =>
            logical.MapInPandas(
              pythonUdf,
              pythonUdf.dataType.asInstanceOf[StructType].toAttributes,
              baseRel,
              isBarrier)
          case PythonEvalType.SQL_MAP_ARROW_ITER_UDF =>
            logical.PythonMapInArrow(
              pythonUdf,
              pythonUdf.dataType.asInstanceOf[StructType].toAttributes,
              baseRel,
              isBarrier)
          case _ =>
            throw InvalidPlanInput(
              s"Function with EvalType: ${pythonUdf.evalType} is not supported")
        }
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${commonUdf.getFunctionCase.getNumber} is not supported")
    }
  }

  private def generateObjAttr[T](enc: ExpressionEncoder[T]): Attribute = {
    val dataType = enc.deserializer.dataType
    val nullable = !enc.clsTag.runtimeClass.isPrimitive
    AttributeReference("obj", dataType, nullable)()
  }

  private def transformTypedMapPartitions(
      fun: proto.CommonInlineUserDefinedFunction,
      child: LogicalPlan): LogicalPlan = {
    val udf = fun.getScalarScalaUdf
    val udfPacket =
      Utils.deserialize[UdfPacket](
        udf.getPayload.toByteArray,
        SparkConnectArtifactManager.classLoaderWithArtifacts)
    assert(udfPacket.inputEncoders.size == 1)
    val iEnc = ExpressionEncoder(udfPacket.inputEncoders.head)
    val rEnc = ExpressionEncoder(udfPacket.outputEncoder)

    val deserializer = UnresolvedDeserializer(iEnc.deserializer)
    val deserialized = DeserializeToObject(deserializer, generateObjAttr(iEnc), child)
    val mapped = MapPartitions(
      udfPacket.function.asInstanceOf[Iterator[Any] => Iterator[Any]],
      generateObjAttr(rEnc),
      deserialized)
    SerializeFromObject(rEnc.namedExpressions, mapped)
  }

  private def transformGroupMap(rel: proto.GroupMap): LogicalPlan = {
    val pythonUdf = exprTransformer.transformPythonUDF(rel.getFunc)
    val cols =
      rel.getGroupingExpressionsList.asScala.toSeq.map(expr => Column(transformExpression(expr)))

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .groupBy(cols: _*)
      .flatMapGroupsInPandas(pythonUdf)
      .logicalPlan
  }

  private def transformCoGroupMap(rel: proto.CoGroupMap): LogicalPlan = {
    val pythonUdf = exprTransformer.transformPythonUDF(rel.getFunc)

    val inputCols =
      rel.getInputGroupingExpressionsList.asScala.toSeq.map(expr =>
        Column(transformExpression(expr)))
    val otherCols =
      rel.getOtherGroupingExpressionsList.asScala.toSeq.map(expr =>
        Column(transformExpression(expr)))

    val input = Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .groupBy(inputCols: _*)
    val other = Dataset
      .ofRows(session, transformRelation(rel.getOther))
      .groupBy(otherCols: _*)

    input.flatMapCoGroupsInPandas(other, pythonUdf).logicalPlan
  }

  private def transformApplyInPandasWithState(rel: proto.ApplyInPandasWithState): LogicalPlan = {
    val pythonUdf = exprTransformer.transformPythonUDF(rel.getFunc)
    val cols =
      rel.getGroupingExpressionsList.asScala.toSeq.map(expr => Column(transformExpression(expr)))

    val outputSchema = parseSchema(rel.getOutputSchema)

    val stateSchema = parseSchema(rel.getStateSchema)

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .groupBy(cols: _*)
      .applyInPandasWithState(
        pythonUdf,
        outputSchema,
        stateSchema,
        rel.getOutputMode,
        rel.getTimeoutConf)
      .logicalPlan
  }

  private def transformWithColumnsRenamed(rel: proto.WithColumnsRenamed): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .withColumnsRenamed(rel.getRenameColumnsMapMap)
      .logicalPlan
  }

  private def transformWithColumns(rel: proto.WithColumns): LogicalPlan = {
    val (colNames, cols, metadata) =
      rel.getAliasesList.asScala.toSeq.map { alias =>
        if (alias.getNameCount != 1) {
          throw InvalidPlanInput(s"""WithColumns require column name only contains one name part,
             |but got ${alias.getNameList.toString}""".stripMargin)
        }

        val metadata = if (alias.hasMetadata && alias.getMetadata.nonEmpty) {
          Metadata.fromJson(alias.getMetadata)
        } else {
          Metadata.empty
        }

        (alias.getName(0), Column(transformExpression(alias.getExpr)), metadata)
      }.unzip3

    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .withColumns(colNames, cols, metadata)
      .logicalPlan
  }

  private def transformWithWatermark(rel: proto.WithWatermark): LogicalPlan = {
    Dataset
      .ofRows(session, transformRelation(rel.getInput))
      .withWatermark(rel.getEventTime, rel.getDelayThreshold)
      .logicalPlan
  }

  private def transformHint(rel: proto.Hint): LogicalPlan = {

    def extractValue(expr: Expression): Any = {
      expr match {
        case Literal(s, StringType) if s != null =>
          UnresolvedAttribute.quotedString(s.toString)
        case literal: Literal => literal.value
        case UnresolvedFunction(Seq("array"), arguments, _, _, _) =>
          arguments.map(extractValue).toArray
        case other =>
          throw InvalidPlanInput(
            s"Expression should be a Literal or CreateMap or CreateArray, " +
              s"but got ${other.getClass} $other")
      }
    }

    val params = rel.getParametersList.asScala.toSeq.map(transformExpression).map(extractValue)
    UnresolvedHint(rel.getName, params, transformRelation(rel.getInput))
  }

  private def transformUnpivot(rel: proto.Unpivot): LogicalPlan = {
    val ids = rel.getIdsList.asScala.toArray.map { expr =>
      Column(transformExpression(expr))
    }

    if (!rel.hasValues) {
      Unpivot(
        Some(ids.map(_.named)),
        None,
        None,
        rel.getVariableColumnName,
        Seq(rel.getValueColumnName),
        transformRelation(rel.getInput))
    } else {
      val values = rel.getValues.getValuesList.asScala.toArray.map { expr =>
        Column(transformExpression(expr))
      }

      Unpivot(
        Some(ids.map(_.named)),
        Some(values.map(v => Seq(v.named))),
        None,
        rel.getVariableColumnName,
        Seq(rel.getValueColumnName),
        transformRelation(rel.getInput))
    }
  }

  private def transformRepartitionByExpression(
      rel: proto.RepartitionByExpression): LogicalPlan = {
    val numPartitionsOpt = if (rel.hasNumPartitions) {
      Some(rel.getNumPartitions)
    } else {
      None
    }
    val partitionExpressions = rel.getPartitionExprsList.asScala.map(transformExpression).toSeq
    logical.RepartitionByExpression(
      partitionExpressions,
      transformRelation(rel.getInput),
      numPartitionsOpt)
  }

  private def transformCollectMetrics(rel: proto.CollectMetrics): LogicalPlan = {
    val metrics = rel.getMetricsList.asScala.toSeq.map { expr =>
      Column(transformExpression(expr))
    }

    CollectMetrics(rel.getName, metrics.map(_.named), transformRelation(rel.getInput))
  }

  private def transformDeduplicate(rel: proto.Deduplicate): LogicalPlan = {
    if (!rel.hasInput) {
      throw InvalidPlanInput("Deduplicate needs a plan input")
    }
    if (rel.getAllColumnsAsKeys && rel.getColumnNamesCount > 0) {
      throw InvalidPlanInput("Cannot deduplicate on both all columns and a subset of columns")
    }
    if (!rel.getAllColumnsAsKeys && rel.getColumnNamesCount == 0) {
      throw InvalidPlanInput(
        "Deduplicate requires to either deduplicate on all columns or a subset of columns")
    }
    val queryExecution = new QueryExecution(session, transformRelation(rel.getInput))
    val resolver = session.sessionState.analyzer.resolver
    val allColumns = queryExecution.analyzed.output
    if (rel.getAllColumnsAsKeys) {
      if (rel.getWithinWatermark) DeduplicateWithinWatermark(allColumns, queryExecution.analyzed)
      else Deduplicate(allColumns, queryExecution.analyzed)
    } else {
      val toGroupColumnNames = rel.getColumnNamesList.asScala.toSeq
      val groupCols = toGroupColumnNames.flatMap { (colName: String) =>
        // It is possibly there are more than one columns with the same name,
        // so we call filter instead of find.
        val cols = allColumns.filter(col => resolver(col.name, colName))
        if (cols.isEmpty) {
          throw InvalidPlanInput(s"Invalid deduplicate column ${colName}")
        }
        cols
      }
      if (rel.getWithinWatermark) DeduplicateWithinWatermark(groupCols, queryExecution.analyzed)
      else Deduplicate(groupCols, queryExecution.analyzed)
    }
  }

  private[connect] def transformDataType(t: proto.DataType): DataType = {
    t.getKindCase match {
      case proto.DataType.KindCase.UNPARSED =>
        parseDatatypeString(t.getUnparsed.getDataTypeString)
      case _ => DataTypeProtoConverter.toCatalystType(t)
    }
  }

  private[connect] def parseDatatypeString(sqlText: String): DataType = {
    val parser = session.sessionState.sqlParser
    try {
      parser.parseTableSchema(sqlText)
    } catch {
      case e: ParseException =>
        try {
          parser.parseDataType(sqlText)
        } catch {
          case _: ParseException =>
            try {
              parser.parseDataType(s"struct<${sqlText.trim}>")
            } catch {
              case _: ParseException =>
                throw e
            }
        }
    }
  }

  private def transformLocalRelation(rel: proto.LocalRelation): LogicalPlan = {
    var schema: StructType = null
    if (rel.hasSchema) {
      val schemaType = DataType.parseTypeWithFallback(
        rel.getSchema,
        parseDatatypeString,
        fallbackParser = DataType.fromJson)
      schema = schemaType match {
        case s: StructType => s
        case d => StructType(Seq(StructField("value", d)))
      }
    }

    if (rel.hasData) {
      val (rows, structType) = ArrowConverters.fromBatchWithSchemaIterator(
        Iterator(rel.getData.toByteArray),
        TaskContext.get())
      if (structType == null) {
        throw InvalidPlanInput(s"Input data for LocalRelation does not produce a schema.")
      }
      val attributes = structType.toAttributes
      val proj = UnsafeProjection.create(attributes, attributes)
      val data = rows.map(proj)

      if (schema == null) {
        logical.LocalRelation(attributes, data.map(_.copy()).toSeq)
      } else {
        def normalize(dt: DataType): DataType = dt match {
          case udt: UserDefinedType[_] => normalize(udt.sqlType)
          case StructType(fields) =>
            val newFields = fields.zipWithIndex.map {
              case (StructField(_, dataType, nullable, metadata), i) =>
                StructField(s"col_$i", normalize(dataType), nullable, metadata)
            }
            StructType(newFields)
          case ArrayType(elementType, containsNull) =>
            ArrayType(normalize(elementType), containsNull)
          case MapType(keyType, valueType, valueContainsNull) =>
            MapType(normalize(keyType), normalize(valueType), valueContainsNull)
          case _ => dt
        }

        val normalized = normalize(schema).asInstanceOf[StructType]

        val project = Dataset
          .ofRows(
            session,
            logicalPlan =
              logical.LocalRelation(normalize(structType).asInstanceOf[StructType].toAttributes))
          .toDF(normalized.names: _*)
          .to(normalized)
          .logicalPlan
          .asInstanceOf[Project]

        val proj = UnsafeProjection.create(project.projectList, project.child.output)
        logical.LocalRelation(schema.toAttributes, data.map(proj).map(_.copy()).toSeq)
      }
    } else {
      if (schema == null) {
        throw InvalidPlanInput(
          s"Schema for LocalRelation is required when the input data is not provided.")
      }
      LocalRelation(schema.toAttributes, data = Seq.empty)
    }
  }

  /** Parse as DDL, with a fallback to JSON. Throws an exception if if fails to parse. */
  private def parseSchema(schema: String): StructType = {
    DataType.parseTypeWithFallback(
      schema,
      StructType.fromDDL,
      fallbackParser = DataType.fromJson) match {
      case s: StructType => s
      case other => throw InvalidPlanInput(s"Invalid schema $other")
    }
  }

  private def transformReadRel(rel: proto.Read): LogicalPlan = {

    rel.getReadTypeCase match {
      case proto.Read.ReadTypeCase.NAMED_TABLE =>
        val multipartIdentifier =
          CatalystSqlParser.parseMultipartIdentifier(rel.getNamedTable.getUnparsedIdentifier)
        UnresolvedRelation(
          multipartIdentifier,
          new CaseInsensitiveStringMap(rel.getNamedTable.getOptionsMap),
          isStreaming = rel.getIsStreaming)

      case proto.Read.ReadTypeCase.DATA_SOURCE if !rel.getIsStreaming =>
        val localMap = CaseInsensitiveMap[String](rel.getDataSource.getOptionsMap.asScala.toMap)
        val reader = session.read
        if (rel.getDataSource.hasFormat) {
          reader.format(rel.getDataSource.getFormat)
        }
        localMap.foreach { case (key, value) => reader.option(key, value) }
        if (rel.getDataSource.getFormat == "jdbc" && rel.getDataSource.getPredicatesCount > 0) {
          if (!localMap.contains(JDBCOptions.JDBC_URL) ||
            !localMap.contains(JDBCOptions.JDBC_TABLE_NAME)) {
            throw InvalidPlanInput(s"Invalid jdbc params, please specify jdbc url and table.")
          }

          val url = rel.getDataSource.getOptionsMap.get(JDBCOptions.JDBC_URL)
          val table = rel.getDataSource.getOptionsMap.get(JDBCOptions.JDBC_TABLE_NAME)
          val options = new JDBCOptions(url, table, localMap)
          val predicates = rel.getDataSource.getPredicatesList.asScala.toArray
          val parts: Array[Partition] = predicates.zipWithIndex.map { case (part, i) =>
            JDBCPartition(part, i): Partition
          }
          val relation = JDBCRelation(parts, options)(session)
          LogicalRelation(relation)
        } else if (rel.getDataSource.getPredicatesCount == 0) {
          if (rel.getDataSource.hasSchema && rel.getDataSource.getSchema.nonEmpty) {
            reader.schema(parseSchema(rel.getDataSource.getSchema))
          }
          if (rel.getDataSource.getPathsCount == 0) {
            reader.load().queryExecution.analyzed
          } else if (rel.getDataSource.getPathsCount == 1) {
            reader.load(rel.getDataSource.getPaths(0)).queryExecution.analyzed
          } else {
            reader.load(rel.getDataSource.getPathsList.asScala.toSeq: _*).queryExecution.analyzed
          }
        } else {
          throw InvalidPlanInput(
            s"Predicates are not supported for ${rel.getDataSource.getFormat} data sources.")
        }

      case proto.Read.ReadTypeCase.DATA_SOURCE if rel.getIsStreaming =>
        val streamSource = rel.getDataSource
        val reader = session.readStream
        if (streamSource.hasFormat) {
          reader.format(streamSource.getFormat)
        }
        reader.options(streamSource.getOptionsMap.asScala)
        if (streamSource.getSchema.nonEmpty) {
          reader.schema(parseSchema(streamSource.getSchema))
        }
        val streamDF = streamSource.getPathsCount match {
          case 0 => reader.load()
          case 1 => reader.load(streamSource.getPaths(0))
          case _ =>
            throw InvalidPlanInput(s"Multiple paths are not supported for streaming source")
        }

        streamDF.queryExecution.analyzed

      case _ => throw InvalidPlanInput(s"Does not support ${rel.getReadTypeCase.name()}")
    }
  }

  private def transformParse(rel: proto.Parse): LogicalPlan = {
    def dataFrameReader = {
      val localMap = CaseInsensitiveMap[String](rel.getOptionsMap.asScala.toMap)
      val reader = session.read
      if (rel.hasSchema) {
        DataTypeProtoConverter.toCatalystType(rel.getSchema) match {
          case s: StructType => reader.schema(s)
          case other => throw InvalidPlanInput(s"Invalid schema dataType $other")
        }
      }
      localMap.foreach { case (key, value) => reader.option(key, value) }
      reader
    }
    def ds: Dataset[String] = Dataset(session, transformRelation(rel.getInput))(Encoders.STRING)

    rel.getFormat match {
      case proto.Parse.ParseFormat.PARSE_FORMAT_CSV =>
        dataFrameReader.csv(ds).queryExecution.analyzed
      case proto.Parse.ParseFormat.PARSE_FORMAT_JSON =>
        dataFrameReader.json(ds).queryExecution.analyzed
      case _ => throw InvalidPlanInput("Does not support " + rel.getFormat.name())
    }
  }

  private def transformFilter(rel: proto.Filter): LogicalPlan = {
    assert(rel.hasInput)
    val baseRel = transformRelation(rel.getInput)
    val cond = rel.getCondition
    cond.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.COMMON_INLINE_USER_DEFINED_FUNCTION
          if isTypedFilter(cond.getCommonInlineUserDefinedFunction) =>
        transformTypedFilter(cond.getCommonInlineUserDefinedFunction, baseRel)
      case _ =>
        logical.Filter(condition = transformExpression(cond), child = baseRel)
    }
  }

  private def isTypedFilter(udf: proto.CommonInlineUserDefinedFunction): Boolean = {
    // It is a scala udf && the udf argument is an unresolved start.
    // This means the udf is a typed filter to filter on all inputs
    udf.getFunctionCase == proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF &&
    udf.getArgumentsCount == 1 &&
    udf.getArguments(0).getExprTypeCase == proto.Expression.ExprTypeCase.UNRESOLVED_STAR
  }

  private def transformTypedFilter(
      fun: proto.CommonInlineUserDefinedFunction,
      child: LogicalPlan): TypedFilter = {
    val udf = fun.getScalarScalaUdf
    val udfPacket =
      Utils.deserialize[UdfPacket](
        udf.getPayload.toByteArray,
        SparkConnectArtifactManager.classLoaderWithArtifacts)
    assert(udfPacket.inputEncoders.size == 1)
    val iEnc = ExpressionEncoder(udfPacket.inputEncoders.head)
    TypedFilter(udfPacket.function, child)(iEnc)
  }

  private def transformProject(rel: proto.Project): LogicalPlan = {
    val baseRel = if (rel.hasInput) {
      transformRelation(rel.getInput)
    } else {
      logical.OneRowRelation()
    }

    val projection = rel.getExpressionsList.asScala.toSeq
      .map(transformExpression)
      .map(toNamedExpression)

    logical.Project(projectList = projection, child = baseRel)
  }

  private def transformLimit(limit: proto.Limit): LogicalPlan = {
    logical.Limit(
      limitExpr = expressions.Literal(limit.getLimit, IntegerType),
      transformRelation(limit.getInput))
  }

  private def transformTail(tail: proto.Tail): LogicalPlan = {
    logical.Tail(
      limitExpr = expressions.Literal(tail.getLimit, IntegerType),
      transformRelation(tail.getInput))
  }

  private def transformOffset(offset: proto.Offset): LogicalPlan = {
    logical.Offset(
      offsetExpr = expressions.Literal(offset.getOffset, IntegerType),
      transformRelation(offset.getInput))
  }

  private def transformSetOperation(u: proto.SetOperation): LogicalPlan = {
    if (!u.hasLeftInput || !u.hasRightInput) {
      throw InvalidPlanInput("Set operation must have 2 inputs")
    }
    val leftPlan = transformRelation(u.getLeftInput)
    val rightPlan = transformRelation(u.getRightInput)
    val isAll = if (u.hasIsAll) u.getIsAll else false

    u.getSetOpType match {
      case proto.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT =>
        if (u.getByName) {
          throw InvalidPlanInput("Except does not support union_by_name")
        }
        Except(leftPlan, rightPlan, isAll)
      case proto.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT =>
        if (u.getByName) {
          throw InvalidPlanInput("Intersect does not support union_by_name")
        }
        Intersect(leftPlan, rightPlan, isAll)
      case proto.SetOperation.SetOpType.SET_OP_TYPE_UNION =>
        if (!u.getByName && u.getAllowMissingColumns) {
          throw InvalidPlanInput(
            "UnionByName `allowMissingCol` can be true only if `byName` is true.")
        }
        val union = Union(Seq(leftPlan, rightPlan), u.getByName, u.getAllowMissingColumns)
        if (isAll) {
          union
        } else {
          logical.Distinct(union)
        }

      case _ =>
        throw InvalidPlanInput(s"Unsupported set operation ${u.getSetOpTypeValue}")
    }
  }

  private def transformJoin(rel: proto.Join): LogicalPlan = {
    assert(rel.hasLeft && rel.hasRight, "Both join sides must be present")
    if (rel.hasJoinCondition && rel.getUsingColumnsCount > 0) {
      throw InvalidPlanInput(
        s"Using columns or join conditions cannot be set at the same time in Join")
    }
    val joinCondition =
      if (rel.hasJoinCondition) Some(transformExpression(rel.getJoinCondition)) else None
    val catalystJointype = transformJoinType(
      if (rel.getJoinType != null) rel.getJoinType else proto.Join.JoinType.JOIN_TYPE_INNER)
    val joinType = if (rel.getUsingColumnsCount > 0) {
      UsingJoin(catalystJointype, rel.getUsingColumnsList.asScala.toSeq)
    } else {
      catalystJointype
    }
    logical.Join(
      left = transformRelation(rel.getLeft),
      right = transformRelation(rel.getRight),
      joinType = joinType,
      condition = joinCondition,
      hint = logical.JoinHint.NONE)
  }

  private def transformJoinType(t: proto.Join.JoinType): JoinType = {
    t match {
      case proto.Join.JoinType.JOIN_TYPE_INNER => Inner
      case proto.Join.JoinType.JOIN_TYPE_LEFT_ANTI => LeftAnti
      case proto.Join.JoinType.JOIN_TYPE_FULL_OUTER => FullOuter
      case proto.Join.JoinType.JOIN_TYPE_LEFT_OUTER => LeftOuter
      case proto.Join.JoinType.JOIN_TYPE_RIGHT_OUTER => RightOuter
      case proto.Join.JoinType.JOIN_TYPE_LEFT_SEMI => LeftSemi
      case proto.Join.JoinType.JOIN_TYPE_CROSS => Cross
      case _ => throw InvalidPlanInput(s"Join type ${t} is not supported")
    }
  }

  private def transformSort(sort: proto.Sort): LogicalPlan = {
    assert(sort.getOrderCount > 0, "'order' must be present and contain elements.")
    logical.Sort(
      child = transformRelation(sort.getInput),
      global = sort.getIsGlobal,
      order = sort.getOrderList.asScala.toSeq.map(exprTransformer.transformSortOrder))
  }

  private def transformDrop(rel: proto.Drop): LogicalPlan = {
    var output = Dataset.ofRows(session, transformRelation(rel.getInput))
    if (rel.getColumnsCount > 0) {
      val cols = rel.getColumnsList.asScala.toSeq.map(expr => Column(transformExpression(expr)))
      output = output.drop(cols.head, cols.tail: _*)
    }
    if (rel.getColumnNamesCount > 0) {
      val colNames = rel.getColumnNamesList.asScala.toSeq
      output = output.drop(colNames: _*)
    }
    output.logicalPlan
  }

  private def transformAggregate(rel: proto.Aggregate): LogicalPlan = {
    if (!rel.hasInput) {
      throw InvalidPlanInput("Aggregate needs a plan input")
    }
    val input = transformRelation(rel.getInput)

    val groupingExprs = rel.getGroupingExpressionsList.asScala.toSeq.map(transformExpression)
    val aggExprs = rel.getAggregateExpressionsList.asScala.toSeq.map(transformExpression)
    val aliasedAgg = (groupingExprs ++ aggExprs).map(toNamedExpression)

    rel.getGroupType match {
      case proto.Aggregate.GroupType.GROUP_TYPE_GROUPBY =>
        logical.Aggregate(
          groupingExpressions = groupingExprs,
          aggregateExpressions = aliasedAgg,
          child = input)

      case proto.Aggregate.GroupType.GROUP_TYPE_ROLLUP =>
        logical.Aggregate(
          groupingExpressions = Seq(Rollup(groupingExprs.map(Seq(_)))),
          aggregateExpressions = aliasedAgg,
          child = input)

      case proto.Aggregate.GroupType.GROUP_TYPE_CUBE =>
        logical.Aggregate(
          groupingExpressions = Seq(Cube(groupingExprs.map(Seq(_)))),
          aggregateExpressions = aliasedAgg,
          child = input)

      case proto.Aggregate.GroupType.GROUP_TYPE_PIVOT =>
        if (!rel.hasPivot) {
          throw InvalidPlanInput("Aggregate with GROUP_TYPE_PIVOT requires a Pivot")
        }

        val pivotExpr = transformExpression(rel.getPivot.getCol)

        var valueExprs = rel.getPivot.getValuesList.asScala.toSeq
          .map(exprTransformer.transformLiteral)
        if (valueExprs.isEmpty) {
          // This is to prevent unintended OOM errors when the number of distinct values is large
          val maxValues = session.sessionState.conf.dataFramePivotMaxValues
          // Get the distinct values of the column and sort them so its consistent
          val pivotCol = Column(pivotExpr)
          valueExprs = Dataset
            .ofRows(session, input)
            .select(pivotCol)
            .distinct()
            .limit(maxValues + 1)
            .sort(pivotCol) // ensure that the output columns are in a consistent logical order
            .collect()
            .map(_.get(0))
            .toSeq
            .map(expressions.Literal.apply)
        }

        logical.Pivot(
          groupByExprsOpt = Some(groupingExprs.map(toNamedExpression)),
          pivotColumn = pivotExpr,
          pivotValues = valueExprs,
          aggregates = aggExprs,
          child = input)

      case other => throw InvalidPlanInput(s"Unknown Group Type $other")
    }
  }

  private def toNamedExpression(expr: Expression): NamedExpression = expr match {
    case named: NamedExpression => named
    case expr => UnresolvedAlias(expr)
  }

  /**
   * Transforms an input protobuf expression into the Catalyst expression. This is usually not
   * called directly. Typically the planner will traverse the expressions automatically, only
   * plugins are expected to manually perform expression transformations.
   *
   * @param exp
   *   the input expression
   * @return
   *   Catalyst expression
   */
  def transformExpression(exp: proto.Expression): Expression = {
    exprTransformer.transform(exp)
  }

  def process(
      command: proto.Command,
      sessionId: String,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    new SparkConnectCommandHandler(this).handle(command, sessionId, responseObserver)
  }
}
