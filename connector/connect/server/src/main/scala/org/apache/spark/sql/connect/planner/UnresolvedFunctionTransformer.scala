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

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.ml.{functions => MLFunctions}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.avro.{AvroDataToCatalyst, CatalystDataToAvro}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.protobuf.{CatalystDataToProtobuf, ProtobufDataToCatalyst}
import org.apache.spark.sql.types._

/**
 * Helper object used to transforms an input protobuf UnresolvedFunction into the Catalyst
 * expression.
 */
object UnresolvedFunctionTransformer {

  private[connect] def transform(f: proto.Expression.UnresolvedFunction)(
      p: SparkConnectPlanner): Expression = {
    transformUnregisteredFunction(f)(p)
      .getOrElse(transformUnresolvedFunction(f)(p))
  }

  /**
   * Translates a scalar function from proto to the Catalyst expression.
   */
  private def transformUnresolvedFunction(f: proto.Expression.UnresolvedFunction)(
      p: SparkConnectPlanner): UnresolvedFunction = {
    if (f.getIsUserDefinedFunction) {
      UnresolvedFunction(
        p.parser.parseFunctionIdentifier(f.getFunctionName),
        f.getArgumentsList.asScala.map(p.transformExpression).toSeq,
        isDistinct = f.getIsDistinct)
    } else {
      UnresolvedFunction(
        FunctionIdentifier(f.getFunctionName),
        f.getArgumentsList.asScala.map(p.transformExpression).toSeq,
        isDistinct = f.getIsDistinct)
    }
  }

  /**
   * For some reason, not all functions are registered in 'FunctionRegistry'. For a unregistered
   * function, we can still wrap it under the proto 'UnresolvedFunction', and then resolve it in
   * this method.
   */
  private def transformUnregisteredFunction(f: proto.Expression.UnresolvedFunction)(
      p: SparkConnectPlanner): Option[Expression] = f.getFunctionName match {
    case "product" if f.getArgumentsCount == 1 =>
      val product = Product(p.transformExpression(f.getArguments(0)))
      Some(product.toAggregateExpression())

    case "when" if f.getArgumentsCount > 0 =>
      val children = f.getArgumentsList.asScala.toSeq.map(p.transformExpression)
      Some(CaseWhen.createFromParser(children))

    case "in" if f.getArgumentsCount > 0 =>
      val children = f.getArgumentsList.asScala.toSeq.map(p.transformExpression)
      Some(In(children(0), children.tail))

    case "nth_value" if f.getArgumentsCount == 3 =>
      // NthValue does not have a constructor which accepts Expression typed 'ignoreNulls'
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val ignoreNulls = extractBoolean(children(2), "ignoreNulls")
      Some(NthValue(children(0), children(1), ignoreNulls))

    case "like" if f.getArgumentsCount == 3 =>
      // Like does not have a constructor which accepts Expression typed 'escapeChar'
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val escapeChar = extractString(children(2), "escapeChar")
      Some(Like(children(0), children(1), escapeChar.charAt(0)))

    case "ilike" if f.getArgumentsCount == 3 =>
      // ILike does not have a constructor which accepts Expression typed 'escapeChar'
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val escapeChar = extractString(children(2), "escapeChar")
      Some(ILike(children(0), children(1), escapeChar.charAt(0)))

    case "lag" if f.getArgumentsCount == 4 =>
      // Lag does not have a constructor which accepts Expression typed 'ignoreNulls'
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val ignoreNulls = extractBoolean(children(3), "ignoreNulls")
      Some(Lag(children(0), children(1), children(2), ignoreNulls))

    case "lead" if f.getArgumentsCount == 4 =>
      // Lead does not have a constructor which accepts Expression typed 'ignoreNulls'
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val ignoreNulls = extractBoolean(children(3), "ignoreNulls")
      Some(Lead(children(0), children(1), children(2), ignoreNulls))

    case "uuid" if f.getArgumentsCount == 1 =>
      // Uuid does not have a constructor which accepts Expression typed 'seed'
      val seed = extractLong(p.transformExpression(f.getArguments(0)), "seed")
      Some(Uuid(Some(seed)))

    case "shuffle" if f.getArgumentsCount == 2 =>
      // Shuffle does not have a constructor which accepts Expression typed 'seed'
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val seed = extractLong(children(1), "seed")
      Some(Shuffle(children(0), Some(seed)))

    case "bloom_filter_agg" if f.getArgumentsCount == 3 =>
      // [col, expectedNumItems: Long, numBits: Long]
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val bloomFilterAgg = new BloomFilterAggregate(children(0), children(1), children(2))
      Some(bloomFilterAgg.toAggregateExpression())

    case "window" if Seq(2, 3, 4).contains(f.getArgumentsCount) =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      transformTimeWindow(children.toSeq)

    case "session_window" if f.getArgumentsCount == 2 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      transformSessionWindow(children.toSeq)

    case "bucket" if f.getArgumentsCount == 2 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      (children(0), children(1)) match {
        case (numBuckets: Literal, child) if numBuckets.dataType == IntegerType =>
          Some(Bucket(numBuckets, child))
        case (other, _) =>
          throw InvalidPlanInput(s"numBuckets should be a literal integer, but got $other")
      }

    case "years" if f.getArgumentsCount == 1 =>
      Some(Years(p.transformExpression(f.getArguments(0))))

    case "months" if f.getArgumentsCount == 1 =>
      Some(Months(p.transformExpression(f.getArguments(0))))

    case "days" if f.getArgumentsCount == 1 =>
      Some(Days(p.transformExpression(f.getArguments(0))))

    case "hours" if f.getArgumentsCount == 1 =>
      Some(Hours(p.transformExpression(f.getArguments(0))))

    case "unwrap_udt" if f.getArgumentsCount == 1 =>
      Some(UnwrapUDT(p.transformExpression(f.getArguments(0))))

    case "from_json" if Seq(2, 3).contains(f.getArgumentsCount) =>
      // JsonToStructs constructor doesn't accept JSON-formatted schema.
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      transformJsonToStructs(children.toSeq)

    // ML-specific functions
    case "vector_to_array" if f.getArgumentsCount == 2 =>
      val expr = p.transformExpression(f.getArguments(0))
      val dtype = extractString(p.transformExpression(f.getArguments(1)), "dtype")
      dtype match {
        case "float64" =>
          transformUnregisteredUDF(MLFunctions.vectorToArrayUdf, Seq(expr))
        case "float32" =>
          transformUnregisteredUDF(MLFunctions.vectorToArrayFloatUdf, Seq(expr))
        case other =>
          throw InvalidPlanInput(s"Unsupported dtype: $other. Valid values: float64, float32.")
      }

    case "array_to_vector" if f.getArgumentsCount == 1 =>
      val expr = p.transformExpression(f.getArguments(0))
      transformUnregisteredUDF(MLFunctions.arrayToVectorUdf, Seq(expr))

    // PS(Pandas API on Spark)-specific functions
    case "distributed_sequence_id" if f.getArgumentsCount == 0 =>
      Some(DistributedSequenceID())

    case "pandas_product" if f.getArgumentsCount == 2 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val pandasProduct = PandasProduct(children(0), extractBoolean(children(1), "dropna"))
      Some(pandasProduct.toAggregateExpression())

    case "pandas_stddev" if f.getArgumentsCount == 2 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val pandasStddev = PandasStddev(children(0), extractInteger(children(1), "ddof"))
      Some(pandasStddev.toAggregateExpression())

    case "pandas_skew" if f.getArgumentsCount == 1 =>
      val pandasSkew = PandasSkewness(p.transformExpression(f.getArguments(0)))
      Some(pandasSkew.toAggregateExpression())

    case "pandas_kurt" if f.getArgumentsCount == 1 =>
      val pandasKurt = PandasKurtosis(p.transformExpression(f.getArguments(0)))
      Some(pandasKurt.toAggregateExpression())

    case "pandas_var" if f.getArgumentsCount == 2 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val pandasVar = PandasVariance(children(0), extractInteger(children(1), "ddof"))
      Some(pandasVar.toAggregateExpression())

    case "pandas_covar" if f.getArgumentsCount == 3 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val pandasCovar = PandasCovar(children(0), children(1), extractInteger(children(2), "ddof"))
      Some(pandasCovar.toAggregateExpression())

    case "pandas_mode" if f.getArgumentsCount == 2 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val pandasMode = PandasMode(children(0), extractBoolean(children(1), "ignoreNA"))
      Some(pandasMode.toAggregateExpression())

    case "ewm" if f.getArgumentsCount == 3 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val alpha = extractDouble(children(1), "alpha")
      val ignoreNA = extractBoolean(children(2), "ignoreNA")
      Some(EWM(children(0), alpha, ignoreNA))

    case "null_index" if f.getArgumentsCount == 1 =>
      Some(NullIndex(p.transformExpression(f.getArguments(0))))

    case "timestampdiff" if f.getArgumentsCount == 3 =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val unit = extractString(children(0), "unit")
      Some(TimestampDiff(unit, children(1), children(2)))

    // Avro-specific functions
    case "from_avro" if Seq(2, 3).contains(f.getArgumentsCount) =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val jsonFormatSchema = extractString(children(1), "jsonFormatSchema")
      var options = Map.empty[String, String]
      if (f.getArgumentsCount == 3) {
        options = extractMap(children(2), "Options")
      }
      Some(AvroDataToCatalyst(children(0), jsonFormatSchema, options))

    case "to_avro" if Seq(1, 2).contains(f.getArgumentsCount) =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      var jsonFormatSchema = Option.empty[String]
      if (f.getArgumentsCount == 2) {
        jsonFormatSchema = Some(extractString(children(1), "jsonFormatSchema"))
      }
      Some(CatalystDataToAvro(children(0), jsonFormatSchema))

    // Protobuf-specific functions
    case "from_protobuf" if Seq(2, 3, 4).contains(f.getArgumentsCount) =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val (msgName, desc, options) = extractProtobufArgs(children.toSeq)
      Some(ProtobufDataToCatalyst(children(0), msgName, desc, options))

    case "to_protobuf" if Seq(2, 3, 4).contains(f.getArgumentsCount) =>
      val children = f.getArgumentsList.asScala.map(p.transformExpression)
      val (msgName, desc, options) = extractProtobufArgs(children.toSeq)
      Some(CatalystDataToProtobuf(children(0), msgName, desc, options))

    case _ => None
  }

  /**
   * There are some built-in yet not registered UDFs, for example, 'ml.function.array_to_vector'.
   * This method is to convert them to ScalaUDF expressions.
   */
  private def transformUnregisteredUDF(
      udf: org.apache.spark.sql.expressions.UserDefinedFunction,
      exprs: Seq[Expression]) = {
    val f = udf.asInstanceOf[org.apache.spark.sql.expressions.SparkUserDefinedFunction]
    val scalaUDF = ScalaUDF(
      function = f.f,
      dataType = f.dataType,
      children = exprs,
      inputEncoders = f.inputEncoders,
      outputEncoder = f.outputEncoder,
      udfName = f.name,
      nullable = f.nullable,
      udfDeterministic = f.deterministic)
    Some(scalaUDF)
  }

  private def transformTimeWindow(children: Seq[Expression]) = {
    val timeCol = children(0)
    val windowDuration = extractString(children(1), "windowDuration")
    var slideDuration = windowDuration
    if (children.length >= 3) {
      slideDuration = extractString(children(2), "slideDuration")
    }
    var startTime = "0 second"
    if (children.length == 4) {
      startTime = extractString(children(3), "startTime")
    }
    val timeWindow = TimeWindow(timeCol, windowDuration, slideDuration, startTime)
    val alias = Alias(timeWindow, "window")(nonInheritableMetadataKeys =
      Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY))
    Some(alias)
  }

  private def transformSessionWindow(children: Seq[Expression]) = {
    val timeCol = children(0)
    val sessionWindow = children(1) match {
      case Literal(s, StringType) if s != null => SessionWindow(timeCol, s.toString)
      case other => SessionWindow(timeCol, other)
    }
    val alias = Alias(sessionWindow, "session_window")(nonInheritableMetadataKeys =
      Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY))
    Some(alias)
  }

  private def transformJsonToStructs(children: Seq[Expression]) = {
    var schemaOpt = Option.empty[DataType]
    children(1) match {
      case Literal(s, StringType) if s != null =>
        try {
          schemaOpt = Some(DataType.fromJson(s.toString))
        } catch {
          case _: Exception =>
        }
      case _ =>
    }

    schemaOpt.map { schema =>
      var options = Map.empty[String, String]
      if (children.length == 3) {
        options = extractMap(children(2), "Options")
      }
      JsonToStructs(
        schema = CharVarcharUtils.failIfHasCharVarchar(schema),
        options = options,
        child = children(0))
    }
  }

  private def extractProtobufArgs(children: Seq[Expression]) = {
    val msgName = extractString(children(1), "MessageClassName")
    var desc = Option.empty[Array[Byte]]
    var options = Map.empty[String, String]
    if (children.length == 3) {
      children(2) match {
        case b: Literal => desc = Some(extractBinary(b, "binaryFileDescriptorSet"))
        case o => options = extractMap(o, "options")
      }
    } else if (children.length == 4) {
      desc = Some(extractBinary(children(2), "binaryFileDescriptorSet"))
      options = extractMap(children(3), "options")
    }
    (msgName, desc, options)
  }

  private def extractBoolean(expr: Expression, field: String): Boolean = expr match {
    case Literal(bool: Boolean, BooleanType) => bool
    case other => throw InvalidPlanInput(s"$field should be a literal boolean, but got $other")
  }

  private def extractDouble(expr: Expression, field: String): Double = expr match {
    case Literal(double: Double, DoubleType) => double
    case other => throw InvalidPlanInput(s"$field should be a literal double, but got $other")
  }

  private def extractInteger(expr: Expression, field: String): Int = expr match {
    case Literal(int: Int, IntegerType) => int
    case other => throw InvalidPlanInput(s"$field should be a literal integer, but got $other")
  }

  private def extractLong(expr: Expression, field: String): Long = expr match {
    case Literal(long: Long, LongType) => long
    case other => throw InvalidPlanInput(s"$field should be a literal long, but got $other")
  }

  private def extractString(expr: Expression, field: String): String = expr match {
    case Literal(s, StringType) if s != null => s.toString
    case other => throw InvalidPlanInput(s"$field should be a literal string, but got $other")
  }

  private def extractBinary(expr: Expression, field: String): Array[Byte] = expr match {
    case Literal(b: Array[Byte], BinaryType) if b != null => b
    case other => throw InvalidPlanInput(s"$field should be a literal binary, but got $other")
  }

  @scala.annotation.tailrec
  private def extractMap(expr: Expression, field: String): Map[String, String] = expr match {
    case map: CreateMap => ExprUtils.convertToMapData(map)
    case UnresolvedFunction(Seq("map"), args, _, _, _, _) => extractMap(CreateMap(args), field)
    case other => throw InvalidPlanInput(s"$field should be created by map, but got $other")
  }
}
