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

import com.google.common.collect.{Lists, Maps}
import com.google.protobuf.{Any => ProtoAny}

import org.apache.spark.api.python.SimplePythonFunction
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{functions => MLFunctions}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.avro.{AvroDataToCatalyst, CatalystDataToAvro}
import org.apache.spark.sql.catalyst.{expressions, FunctionIdentifier}
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connect.artifact.SparkConnectArtifactManager
import org.apache.spark.sql.connect.common.{InvalidPlanInput, UdfPacket}
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

private[connect] class ExpressionTransformer(planner: SparkConnectPlanner) extends Logging {
  def this(session: SparkSession) = this(new SparkConnectPlanner(session))

  private val session = planner.session

  private lazy val pythonExec =
    sys.env.getOrElse("PYSPARK_PYTHON", sys.env.getOrElse("PYSPARK_DRIVER_PYTHON", "python3"))

  /**
   * Transforms an input protobuf expression into the Catalyst expression. This is usually not
   * called directly. Typically the planner will traverse the expressions automatically, only
   * plugins are expected to manually perform expression transformations.
   *
   * @param expr
   *   the input expression
   * @return
   *   Catalyst expression
   */
  private[connect] def transform(expr: proto.Expression): expressions.Expression = {
    expr.getExprTypeCase match {
      case proto.Expression.ExprTypeCase.LITERAL => transformLiteral(expr.getLiteral)
      case proto.Expression.ExprTypeCase.UNRESOLVED_ATTRIBUTE =>
        transformUnresolvedAttribute(expr.getUnresolvedAttribute)
      case proto.Expression.ExprTypeCase.UNRESOLVED_FUNCTION =>
        transformUnregisteredFunction(expr.getUnresolvedFunction)
          .getOrElse(transformUnresolvedFunction(expr.getUnresolvedFunction))
      case proto.Expression.ExprTypeCase.ALIAS => transformAlias(expr.getAlias)
      case proto.Expression.ExprTypeCase.EXPRESSION_STRING =>
        transformExpressionString(expr.getExpressionString)
      case proto.Expression.ExprTypeCase.UNRESOLVED_STAR =>
        transformUnresolvedStar(expr.getUnresolvedStar)
      case proto.Expression.ExprTypeCase.CAST => transformCast(expr.getCast)
      case proto.Expression.ExprTypeCase.UNRESOLVED_REGEX =>
        transformUnresolvedRegex(expr.getUnresolvedRegex)
      case proto.Expression.ExprTypeCase.UNRESOLVED_EXTRACT_VALUE =>
        transformUnresolvedExtractValue(expr.getUnresolvedExtractValue)
      case proto.Expression.ExprTypeCase.UPDATE_FIELDS =>
        transformUpdateFields(expr.getUpdateFields)
      case proto.Expression.ExprTypeCase.SORT_ORDER => transformSortOrder(expr.getSortOrder)
      case proto.Expression.ExprTypeCase.LAMBDA_FUNCTION =>
        transformLambdaFunction(expr.getLambdaFunction)
      case proto.Expression.ExprTypeCase.UNRESOLVED_NAMED_LAMBDA_VARIABLE =>
        transformUnresolvedNamedLambdaVariable(expr.getUnresolvedNamedLambdaVariable)
      case proto.Expression.ExprTypeCase.WINDOW =>
        transformWindowExpression(expr.getWindow)
      case proto.Expression.ExprTypeCase.EXTENSION =>
        transformExpressionPlugin(expr.getExtension)
      case proto.Expression.ExprTypeCase.COMMON_INLINE_USER_DEFINED_FUNCTION =>
        transformCommonInlineUserDefinedFunction(expr.getCommonInlineUserDefinedFunction)
      case _ =>
        throw InvalidPlanInput(
          s"Expression with ID: ${expr.getExprTypeCase.getNumber} is not supported")
    }
  }

  private def transformUnresolvedAttribute(
      attr: proto.Expression.UnresolvedAttribute): analysis.UnresolvedAttribute = {
    val expr = analysis.UnresolvedAttribute.quotedString(attr.getUnparsedIdentifier)
    if (attr.hasPlanId) {
      expr.setTagValue(logical.LogicalPlan.PLAN_ID_TAG, attr.getPlanId)
    }
    expr
  }

  private def transformExpressionPlugin(extension: ProtoAny): expressions.Expression = {
    SparkConnectPluginRegistry.expressionRegistry
      // Lazily traverse the collection.
      .view
      // Apply the transformation.
      .map(p => p.transform(extension, planner))
      // Find the first non-empty transformation or throw.
      .find(_.nonEmpty)
      .flatten
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
  }

  /**
   * Transforms the protocol buffers literals into the appropriate Catalyst literal expression.
   *
   * @return
   *   Expression
   */
  private[connect] def transformLiteral(lit: proto.Expression.Literal): expressions.Literal = {
    LiteralExpressionProtoConverter.toCatalystExpression(lit)
  }

  /**
   * Translates a scalar function from proto to the Catalyst expression.
   *
   * TODO(SPARK-40546) We need to homogenize the function names for binary operators.
   *
   * @param fun
   *   Proto representation of the function call.
   * @return
   */
  private def transformUnresolvedFunction(
      fun: proto.Expression.UnresolvedFunction): expressions.Expression = {
    if (fun.getIsUserDefinedFunction) {
      analysis.UnresolvedFunction(
        session.sessionState.sqlParser.parseFunctionIdentifier(fun.getFunctionName),
        fun.getArgumentsList.asScala.map(transform).toSeq,
        isDistinct = fun.getIsDistinct)
    } else {
      analysis.UnresolvedFunction(
        FunctionIdentifier(fun.getFunctionName),
        fun.getArgumentsList.asScala.map(transform).toSeq,
        isDistinct = fun.getIsDistinct)
    }
  }

  /**
   * Translates a user-defined function from proto to the Catalyst expression.
   *
   * @param fun
   *   Proto representation of the function call.
   * @return
   *   Expression.
   */
  private def transformCommonInlineUserDefinedFunction(
      fun: proto.CommonInlineUserDefinedFunction): expressions.Expression = {
    fun.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        transformPythonUDF(fun)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
        transformScalarScalaUDF(fun)
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${fun.getFunctionCase.getNumber} is not supported")
    }
  }

  /**
   * Translates a Scalar Scala user-defined function from proto to the Catalyst expression.
   *
   * @param fun
   *   Proto representation of the Scalar Scalar user-defined function.
   * @return
   *   ScalaUDF.
   */
  private def transformScalarScalaUDF(
      fun: proto.CommonInlineUserDefinedFunction): expressions.ScalaUDF = {
    val udf = fun.getScalarScalaUdf
    val udfPacket =
      Utils.deserialize[UdfPacket](
        udf.getPayload.toByteArray,
        SparkConnectArtifactManager.classLoaderWithArtifacts)
    expressions.ScalaUDF(
      function = udfPacket.function,
      dataType = planner.transformDataType(udf.getOutputType),
      children = fun.getArgumentsList.asScala.map(transform).toSeq,
      inputEncoders = udfPacket.inputEncoders.map(e => Option(ExpressionEncoder(e))),
      outputEncoder = Option(ExpressionEncoder(udfPacket.outputEncoder)),
      udfName = Option(fun.getFunctionName),
      nullable = udf.getNullable,
      udfDeterministic = fun.getDeterministic)
  }

  /**
   * Translates a Python user-defined function from proto to the Catalyst expression.
   *
   * @param fun
   *   Proto representation of the Python user-defined function.
   * @return
   *   PythonUDF.
   */
  private[connect] def transformPythonUDF(
      fun: proto.CommonInlineUserDefinedFunction): expressions.PythonUDF = {
    val udf = fun.getPythonUdf
    expressions.PythonUDF(
      name = fun.getFunctionName,
      func = transformPythonFunction(udf),
      dataType = planner.transformDataType(udf.getOutputType),
      children = fun.getArgumentsList.asScala.map(transform).toSeq,
      evalType = udf.getEvalType,
      udfDeterministic = fun.getDeterministic)
  }

  private[connect] def transformPythonFunction(fun: proto.PythonUDF): SimplePythonFunction = {
    SimplePythonFunction(
      command = fun.getCommand.toByteArray,
      // Empty environment variables
      envVars = Maps.newHashMap(),
      // No imported Python libraries
      pythonIncludes = Lists.newArrayList(),
      pythonExec = pythonExec,
      pythonVer = fun.getPythonVer,
      // Empty broadcast variables
      broadcastVars = Lists.newArrayList(),
      // Null accumulator
      accumulator = null)
  }

  /**
   * Translates a LambdaFunction from proto to the Catalyst expression.
   */
  private[connect] def transformLambdaFunction(
      lambda: proto.Expression.LambdaFunction): expressions.LambdaFunction = {
    if (lambda.getArgumentsCount == 0 || lambda.getArgumentsCount > 3) {
      throw InvalidPlanInput(
        "LambdaFunction requires 1 ~ 3 arguments, " +
          s"but got ${lambda.getArgumentsCount} ones!")
    }

    expressions.LambdaFunction(
      function = transform(lambda.getFunction),
      arguments = lambda.getArgumentsList.asScala.toSeq
        .map(transformUnresolvedNamedLambdaVariable))
  }

  private def transformUnresolvedNamedLambdaVariable(
      variable: proto.Expression.UnresolvedNamedLambdaVariable)
      : expressions.UnresolvedNamedLambdaVariable = {
    if (variable.getNamePartsCount == 0) {
      throw InvalidPlanInput("UnresolvedNamedLambdaVariable requires at least one name part!")
    }

    expressions.UnresolvedNamedLambdaVariable(variable.getNamePartsList.asScala.toSeq)
  }

  /**
   * For some reason, not all functions are registered in 'FunctionRegistry'. For a unregistered
   * function, we can still wrap it under the proto 'UnresolvedFunction', and then resolve it in
   * this method.
   */
  private def transformUnregisteredFunction(
      fun: proto.Expression.UnresolvedFunction): Option[expressions.Expression] = {
    fun.getFunctionName match {
      case "product" if fun.getArgumentsCount == 1 =>
        Some(
          expressions.aggregate
            .Product(transform(fun.getArgumentsList.asScala.head))
            .toAggregateExpression())

      case "when" if fun.getArgumentsCount > 0 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        Some(expressions.CaseWhen.createFromParser(children))

      case "in" if fun.getArgumentsCount > 0 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        Some(expressions.In(children.head, children.tail))

      case "nth_value" if fun.getArgumentsCount == 3 =>
        // NthValue does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        val ignoreNulls = extractBoolean(children(2), "ignoreNulls")
        Some(expressions.NthValue(children(0), children(1), ignoreNulls))

      case "lag" if fun.getArgumentsCount == 4 =>
        // Lag does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        val ignoreNulls = extractBoolean(children(3), "ignoreNulls")
        Some(expressions.Lag(children.head, children(1), children(2), ignoreNulls))

      case "lead" if fun.getArgumentsCount == 4 =>
        // Lead does not have a constructor which accepts Expression typed 'ignoreNulls'
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        val ignoreNulls = extractBoolean(children(3), "ignoreNulls")
        Some(expressions.Lead(children.head, children(1), children(2), ignoreNulls))

      case "window" if Seq(2, 3, 4).contains(fun.getArgumentsCount) =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        val timeCol = children.head
        val windowDuration = extractString(children(1), "windowDuration")
        var slideDuration = windowDuration
        if (fun.getArgumentsCount >= 3) {
          slideDuration = extractString(children(2), "slideDuration")
        }
        var startTime = "0 second"
        if (fun.getArgumentsCount == 4) {
          startTime = extractString(children(3), "startTime")
        }
        Some(
          expressions.Alias(
            expressions.TimeWindow(timeCol, windowDuration, slideDuration, startTime),
            "window")(nonInheritableMetadataKeys =
            Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY)))

      case "session_window" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        val timeCol = children.head
        val sessionWindow = children.last match {
          case expressions.Literal(s, StringType) if s != null =>
            expressions.SessionWindow(timeCol, s.toString)
          case other => expressions.SessionWindow(timeCol, other)
        }
        Some(
          expressions.Alias(sessionWindow, "session_window")(nonInheritableMetadataKeys =
            Seq(Dataset.DATASET_ID_KEY, Dataset.COL_POS_KEY)))

      case "bucket" if fun.getArgumentsCount == 2 =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        (children.head, children.last) match {
          case (numBuckets: expressions.Literal, child) if numBuckets.dataType == IntegerType =>
            Some(expressions.Bucket(numBuckets, child))
          case (other, _) =>
            throw InvalidPlanInput(s"numBuckets should be a literal integer, but got $other")
        }

      case "years" if fun.getArgumentsCount == 1 =>
        Some(expressions.Years(transform(fun.getArguments(0))))

      case "months" if fun.getArgumentsCount == 1 =>
        Some(expressions.Months(transform(fun.getArguments(0))))

      case "days" if fun.getArgumentsCount == 1 =>
        Some(expressions.Days(transform(fun.getArguments(0))))

      case "hours" if fun.getArgumentsCount == 1 =>
        Some(expressions.Hours(transform(fun.getArguments(0))))

      case "unwrap_udt" if fun.getArgumentsCount == 1 =>
        Some(expressions.UnwrapUDT(transform(fun.getArguments(0))))

      case "from_json" if Seq(2, 3).contains(fun.getArgumentsCount) =>
        // JsonToStructs constructor doesn't accept JSON-formatted schema.
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)

        var schema: DataType = null
        children(1) match {
          case expressions.Literal(s, StringType) if s != null =>
            try {
              schema = DataType.fromJson(s.toString)
            } catch {
              case _: Exception =>
            }
          case _ =>
        }

        if (schema != null) {
          var options = Map.empty[String, String]
          if (children.length == 3) {
            options = extractMapData(children(2), "Options")
          }
          Some(
            expressions.JsonToStructs(
              schema = CharVarcharUtils.failIfHasCharVarchar(schema),
              options = options,
              child = children.head))
        } else {
          None
        }

      // Avro-specific functions
      case "from_avro" if Seq(2, 3).contains(fun.getArgumentsCount) =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        val jsonFormatSchema = extractString(children(1), "jsonFormatSchema")
        var options = Map.empty[String, String]
        if (fun.getArgumentsCount == 3) {
          options = extractMapData(children(2), "Options")
        }
        Some(AvroDataToCatalyst(children.head, jsonFormatSchema, options))

      case "to_avro" if Seq(1, 2).contains(fun.getArgumentsCount) =>
        val children = fun.getArgumentsList.asScala.toSeq.map(transform)
        var jsonFormatSchema = Option.empty[String]
        if (fun.getArgumentsCount == 2) {
          jsonFormatSchema = Some(extractString(children(1), "jsonFormatSchema"))
        }
        Some(CatalystDataToAvro(children.head, jsonFormatSchema))

      // PS(Pandas API on Spark)-specific functions
      case "distributed_sequence_id" if fun.getArgumentsCount == 0 =>
        Some(expressions.DistributedSequenceID())

      // ML-specific functions
      case "vector_to_array" if fun.getArgumentsCount == 2 =>
        val expr = transform(fun.getArguments(0))
        val dtype = extractString(transform(fun.getArguments(1)), "dtype")
        dtype match {
          case "float64" =>
            Some(transformUnregisteredUDF(MLFunctions.vectorToArrayUdf, Seq(expr)))
          case "float32" =>
            Some(transformUnregisteredUDF(MLFunctions.vectorToArrayFloatUdf, Seq(expr)))
          case other =>
            throw InvalidPlanInput(s"Unsupported dtype: $other. Valid values: float64, float32.")
        }

      case "array_to_vector" if fun.getArgumentsCount == 1 =>
        val expr = transform(fun.getArguments(0))
        Some(transformUnregisteredUDF(MLFunctions.arrayToVectorUdf, Seq(expr)))

      case _ => None
    }
  }

  /**
   * There are some built-in yet not registered UDFs, for example, 'ml.function.array_to_vector'.
   * This method is to convert them to ScalaUDF expressions.
   */
  private def transformUnregisteredUDF(
      fun: org.apache.spark.sql.expressions.UserDefinedFunction,
      exprs: Seq[expressions.Expression]): expressions.ScalaUDF = {
    val f = fun.asInstanceOf[org.apache.spark.sql.expressions.SparkUserDefinedFunction]
    expressions.ScalaUDF(
      function = f.f,
      dataType = f.dataType,
      children = exprs,
      inputEncoders = f.inputEncoders,
      outputEncoder = f.outputEncoder,
      udfName = f.name,
      nullable = f.nullable,
      udfDeterministic = f.deterministic)
  }

  private def extractBoolean(expr: expressions.Expression, field: String): Boolean = expr match {
    case expressions.Literal(bool: Boolean, BooleanType) => bool
    case other => throw InvalidPlanInput(s"$field should be a literal boolean, but got $other")
  }

  private def extractString(expr: expressions.Expression, field: String): String = expr match {
    case expressions.Literal(s, StringType) if s != null => s.toString
    case other => throw InvalidPlanInput(s"$field should be a literal string, but got $other")
  }

  private def extractMapData(expr: expressions.Expression, field: String): Map[String, String] =
    expr match {
      case map: expressions.CreateMap => expressions.ExprUtils.convertToMapData(map)
      case analysis.UnresolvedFunction(Seq("map"), args, _, _, _) =>
        extractMapData(expressions.CreateMap(args), field)
      case other => throw InvalidPlanInput(s"$field should be created by map, but got $other")
    }

  private def transformAlias(alias: proto.Expression.Alias): expressions.NamedExpression = {
    if (alias.getNameCount == 1) {
      val metadata = if (alias.hasMetadata() && alias.getMetadata.nonEmpty) {
        Some(Metadata.fromJson(alias.getMetadata))
      } else {
        None
      }
      expressions.Alias(transform(alias.getExpr), alias.getName(0))(explicitMetadata = metadata)
    } else {
      if (alias.hasMetadata) {
        throw InvalidPlanInput(
          "Alias expressions with more than 1 identifier must not use optional metadata.")
      }
      analysis.MultiAlias(transform(alias.getExpr), alias.getNameList.asScala.toSeq)
    }
  }

  private def transformExpressionString(
      expr: proto.Expression.ExpressionString): expressions.Expression = {
    session.sessionState.sqlParser.parseExpression(expr.getExpression)
  }

  private def transformUnresolvedStar(
      star: proto.Expression.UnresolvedStar): analysis.UnresolvedStar = {
    if (star.hasUnparsedTarget) {
      val target = star.getUnparsedTarget
      if (!target.endsWith(".*")) {
        throw InvalidPlanInput(
          s"UnresolvedStar requires a unparsed target ending with '.*', " +
            s"but got $target.")
      }

      analysis.UnresolvedStar(
        Some(
          analysis.UnresolvedAttribute
            .parseAttributeName(target.substring(0, target.length - 2))))
    } else {
      analysis.UnresolvedStar(None)
    }
  }

  private def transformCast(cast: proto.Expression.Cast): expressions.Expression = {
    cast.getCastToTypeCase match {
      case proto.Expression.Cast.CastToTypeCase.TYPE =>
        expressions.Cast(transform(cast.getExpr), planner.transformDataType(cast.getType))
      case _ =>
        expressions.Cast(
          transform(cast.getExpr),
          session.sessionState.sqlParser.parseDataType(cast.getTypeStr))
    }
  }

  private def transformUnresolvedRegex(
      regex: proto.Expression.UnresolvedRegex): expressions.Expression = {
    val caseSensitive = session.sessionState.conf.caseSensitiveAnalysis
    regex.getColName match {
      case ParserUtils.escapedIdentifier(columnNameRegex) =>
        analysis.UnresolvedRegex(columnNameRegex, None, caseSensitive)
      case ParserUtils.qualifiedEscapedIdentifier(nameParts, columnNameRegex) =>
        analysis.UnresolvedRegex(columnNameRegex, Some(nameParts), caseSensitive)
      case _ =>
        val expr = analysis.UnresolvedAttribute.quotedString(regex.getColName)
        if (regex.hasPlanId) {
          expr.setTagValue(logical.LogicalPlan.PLAN_ID_TAG, regex.getPlanId)
        }
        expr
    }
  }

  private def transformUnresolvedExtractValue(
      extract: proto.Expression.UnresolvedExtractValue): analysis.UnresolvedExtractValue = {
    analysis.UnresolvedExtractValue(transform(extract.getChild), transform(extract.getExtraction))
  }

  private def transformUpdateFields(
      update: proto.Expression.UpdateFields): expressions.UpdateFields = {
    if (update.hasValueExpression) {
      // add or replace a field
      expressions.UpdateFields.apply(
        col = transform(update.getStructExpression),
        fieldName = update.getFieldName,
        expr = transform(update.getValueExpression))
    } else {
      // drop a field
      expressions.UpdateFields.apply(
        col = transform(update.getStructExpression),
        fieldName = update.getFieldName)
    }
  }

  private def transformWindowExpression(window: proto.Expression.Window) = {
    if (!window.hasWindowFunction) {
      throw InvalidPlanInput(s"WindowFunction is required in WindowExpression")
    }

    val frameSpec = if (window.hasFrameSpec) {
      val protoFrameSpec = window.getFrameSpec

      val frameType = protoFrameSpec.getFrameType match {
        case proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW =>
          expressions.RowFrame

        case proto.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE =>
          expressions.RangeFrame

        case other => throw InvalidPlanInput(s"Unknown FrameType $other")
      }

      if (!protoFrameSpec.hasLower) {
        throw InvalidPlanInput(s"LowerBound is required in WindowFrame")
      }
      val lower = protoFrameSpec.getLower.getBoundaryCase match {
        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.CURRENT_ROW =>
          expressions.CurrentRow

        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.UNBOUNDED =>
          expressions.UnboundedPreceding

        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.VALUE =>
          transform(protoFrameSpec.getLower.getValue)

        case other => throw InvalidPlanInput(s"Unknown FrameBoundary $other")
      }

      if (!protoFrameSpec.hasUpper) {
        throw InvalidPlanInput(s"UpperBound is required in WindowFrame")
      }
      val upper = protoFrameSpec.getUpper.getBoundaryCase match {
        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.CURRENT_ROW =>
          expressions.CurrentRow

        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.UNBOUNDED =>
          expressions.UnboundedFollowing

        case proto.Expression.Window.WindowFrame.FrameBoundary.BoundaryCase.VALUE =>
          transform(protoFrameSpec.getUpper.getValue)

        case other => throw InvalidPlanInput(s"Unknown FrameBoundary $other")
      }

      expressions.SpecifiedWindowFrame(frameType = frameType, lower = lower, upper = upper)

    } else {
      expressions.UnspecifiedFrame
    }

    val windowSpec = expressions.WindowSpecDefinition(
      partitionSpec = window.getPartitionSpecList.asScala.toSeq.map(transform),
      orderSpec = window.getOrderSpecList.asScala.toSeq.map(transformSortOrder),
      frameSpecification = frameSpec)

    expressions.WindowExpression(
      windowFunction = transform(window.getWindowFunction),
      windowSpec = windowSpec)
  }

  private[connect] def transformSortOrder(
      order: proto.Expression.SortOrder): expressions.SortOrder = {
    expressions.SortOrder(
      child = transform(order.getChild),
      direction = order.getDirection match {
        case proto.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING =>
          expressions.Ascending
        case _ => expressions.Descending
      },
      nullOrdering = order.getNullOrdering match {
        case proto.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST =>
          expressions.NullsFirst
        case _ => expressions.NullsLast
      },
      sameOrderExpressions = Seq.empty)
  }

}
