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

package org.apache.spark.sql.connect.service

import scala.collection.JavaConverters._

import com.google.protobuf.{Any => ProtoAny, ByteString}
import io.grpc.stub.StreamObserver

import org.apache.spark.SparkEnv
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.analysis.{GlobalTempView, LocalTempView}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.connect.common.InvalidPlanInput
import org.apache.spark.sql.connect.config.Connect.CONNECT_GRPC_ARROW_MAX_BATCH_SIZE
import org.apache.spark.sql.connect.planner.{InvalidCommandInput, SaveModeConverter, SparkConnectPlanner}
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.execution.streaming.StreamingQueryWrapper
import org.apache.spark.sql.streaming.Trigger

private[connect] class SparkConnectCommandHandler(planner: SparkConnectPlanner) extends Logging {
  def this(session: SparkSession) = {
    this(new SparkConnectPlanner(session))
  }

  private def session = planner.session

  def handle(
      command: proto.Command,
      sessionId: String,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    command.getCommandTypeCase match {
      case proto.Command.CommandTypeCase.REGISTER_FUNCTION =>
        handleRegisterUserDefinedFunction(command.getRegisterFunction)
      case proto.Command.CommandTypeCase.WRITE_OPERATION =>
        handleWriteOperation(command.getWriteOperation)
      case proto.Command.CommandTypeCase.CREATE_DATAFRAME_VIEW =>
        handleCreateViewCommand(command.getCreateDataframeView)
      case proto.Command.CommandTypeCase.WRITE_OPERATION_V2 =>
        handleWriteOperationV2(command.getWriteOperationV2)
      case proto.Command.CommandTypeCase.EXTENSION =>
        handleCommandPlugin(command.getExtension)
      case proto.Command.CommandTypeCase.SQL_COMMAND =>
        handleSqlCommand(command.getSqlCommand, sessionId, responseObserver)
      case proto.Command.CommandTypeCase.WRITE_STREAM_OPERATION_START =>
        handleWriteStreamOperationStart(
          command.getWriteStreamOperationStart,
          sessionId,
          responseObserver)
      case proto.Command.CommandTypeCase.STREAMING_QUERY_COMMAND =>
        handleStreamingQueryCommand(command.getStreamingQueryCommand, sessionId, responseObserver)
      case proto.Command.CommandTypeCase.GET_RESOURCES_COMMAND =>
        handleGetResourcesCommand(command.getGetResourcesCommand, sessionId, responseObserver)
      case _ => throw new UnsupportedOperationException(s"$command not supported.")
    }
  }

  private def handleSqlCommand(
      getSqlCommand: proto.SqlCommand,
      sessionId: String,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    // Eagerly execute commands of the provided SQL string.
    val df = session.sql(
      getSqlCommand.getSql,
      getSqlCommand.getArgsMap.asScala.mapValues(planner.transformLiteral).toMap)
    // Check if commands have been executed.
    val isCommand = df.queryExecution.commandExecuted.isInstanceOf[logical.CommandResult]
    val rows = df.logicalPlan match {
      case lr: logical.LocalRelation => lr.data
      case cr: logical.CommandResult => cr.rows
      case _ => Seq.empty
    }

    // Convert the results to Arrow.
    val schema = df.schema
    val maxRecordsPerBatch = session.sessionState.conf.arrowMaxRecordsPerBatch
    val maxBatchSize = (SparkEnv.get.conf.get(CONNECT_GRPC_ARROW_MAX_BATCH_SIZE) * 0.7).toLong
    val timeZoneId = session.sessionState.conf.sessionLocalTimeZone

    // Convert the data.
    val bytes = if (rows.isEmpty) {
      ArrowConverters.createEmptyArrowBatch(schema, timeZoneId)
    } else {
      val batches = ArrowConverters.toBatchWithSchemaIterator(
        rows.iterator,
        schema,
        maxRecordsPerBatch,
        maxBatchSize,
        timeZoneId)
      assert(batches.hasNext)
      val bytes = batches.next()
      assert(!batches.hasNext, s"remaining batches: ${batches.size}")
      bytes
    }

    // To avoid explicit handling of the result on the client, we build the expected input
    // of the relation on the server. The client has to simply forward the result.
    val result = proto.ExecutePlanResponse.SqlCommandResult.newBuilder()
    if (isCommand) {
      result.setRelation(
        proto.Relation
          .newBuilder()
          .setLocalRelation(
            proto.LocalRelation
              .newBuilder()
              .setData(ByteString.copyFrom(bytes))))
    } else {
      result.setRelation(
        proto.Relation
          .newBuilder()
          .setSql(
            proto.SQL
              .newBuilder()
              .setQuery(getSqlCommand.getSql)
              .putAllArgs(getSqlCommand.getArgsMap)))
    }
    // Exactly one SQL Command Result Batch
    responseObserver.onNext(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setSqlCommandResult(result)
        .build())

    // Send Metrics
    responseObserver.onNext(SparkConnectStreamHandler.createMetricsResponse(sessionId, df))
  }

  private def handleRegisterUserDefinedFunction(
      fun: proto.CommonInlineUserDefinedFunction): Unit = {
    fun.getFunctionCase match {
      case proto.CommonInlineUserDefinedFunction.FunctionCase.PYTHON_UDF =>
        handleRegisterPythonUDF(fun)
      case proto.CommonInlineUserDefinedFunction.FunctionCase.JAVA_UDF =>
        handleRegisterJavaUDF(fun)
      case _ =>
        throw InvalidPlanInput(
          s"Function with ID: ${fun.getFunctionCase.getNumber} is not supported")
    }
  }

  private def handleRegisterPythonUDF(fun: proto.CommonInlineUserDefinedFunction): Unit = {
    val udf = fun.getPythonUdf
    val function = planner.transformPythonFunction(udf)
    val udpf = UserDefinedPythonFunction(
      name = fun.getFunctionName,
      func = function,
      dataType = planner.transformDataType(udf.getOutputType),
      pythonEvalType = udf.getEvalType,
      udfDeterministic = fun.getDeterministic)

    session.udf.registerPython(fun.getFunctionName, udpf)
  }

  private def handleRegisterJavaUDF(fun: proto.CommonInlineUserDefinedFunction): Unit = {
    val udf = fun.getJavaUdf
    val dataType = if (udf.hasOutputType) {
      planner.transformDataType(udf.getOutputType)
    } else {
      null
    }
    if (udf.getAggregate) {
      session.udf.registerJavaUDAF(fun.getFunctionName, udf.getClassName)
    } else {
      session.udf.registerJava(fun.getFunctionName, udf.getClassName, dataType)
    }
  }

  private def handleCommandPlugin(extension: ProtoAny): Unit = {
    SparkConnectPluginRegistry.commandRegistry
      // Lazily traverse the collection.
      .view
      // Apply the transformation.
      .map(p => p.process(extension, planner))
      // Find the first non-empty transformation or throw.
      .find(_.nonEmpty)
      .flatten
      .getOrElse(throw InvalidPlanInput("No handler found for extension"))
  }

  private def handleCreateViewCommand(createView: proto.CreateDataFrameViewCommand): Unit = {
    val viewType = if (createView.getIsGlobal) GlobalTempView else LocalTempView

    val tableIdentifier =
      try {
        session.sessionState.sqlParser.parseTableIdentifier(createView.getName)
      } catch {
        case _: ParseException =>
          throw QueryCompilationErrors.invalidViewNameError(createView.getName)
      }

    val plan = CreateViewCommand(
      name = tableIdentifier,
      userSpecifiedColumns = Nil,
      comment = None,
      properties = Map.empty,
      originalText = None,
      plan = planner.transformRelation(createView.getInput),
      allowExisting = false,
      replace = createView.getReplace,
      viewType = viewType)

    Dataset.ofRows(session, plan).queryExecution.commandExecuted
  }

  /**
   * Transforms the write operation and executes it.
   *
   * The input write operation contains a reference to the input plan and transforms it to the
   * corresponding logical plan. Afterwards, creates the DataFrameWriter and translates the
   * parameters of the WriteOperation into the corresponding methods calls.
   *
   * @param writeOperation
   */
  private def handleWriteOperation(writeOperation: proto.WriteOperation): Unit = {
    // Transform the input plan into the logical plan.
    val plan = planner.transformRelation(writeOperation.getInput)
    // And create a Dataset from the plan.
    val dataset = Dataset.ofRows(session, logicalPlan = plan)

    val w = dataset.write
    if (writeOperation.getMode != proto.WriteOperation.SaveMode.SAVE_MODE_UNSPECIFIED) {
      w.mode(SaveModeConverter.toSaveMode(writeOperation.getMode))
    }

    if (writeOperation.getOptionsCount > 0) {
      writeOperation.getOptionsMap.asScala.foreach { case (key, value) => w.option(key, value) }
    }

    if (writeOperation.getSortColumnNamesCount > 0) {
      val names = writeOperation.getSortColumnNamesList.asScala
      w.sortBy(names.head, names.tail.toSeq: _*)
    }

    if (writeOperation.hasBucketBy) {
      val op = writeOperation.getBucketBy
      val cols = op.getBucketColumnNamesList.asScala
      if (op.getNumBuckets <= 0) {
        throw InvalidCommandInput(
          s"BucketBy must specify a bucket count > 0, received ${op.getNumBuckets} instead.")
      }
      w.bucketBy(op.getNumBuckets, cols.head, cols.tail.toSeq: _*)
    }

    if (writeOperation.getPartitioningColumnsCount > 0) {
      val names = writeOperation.getPartitioningColumnsList.asScala
      w.partitionBy(names.toSeq: _*)
    }

    if (writeOperation.hasSource) {
      w.format(writeOperation.getSource)
    }

    writeOperation.getSaveTypeCase match {
      case proto.WriteOperation.SaveTypeCase.SAVETYPE_NOT_SET => w.save()
      case proto.WriteOperation.SaveTypeCase.PATH => w.save(writeOperation.getPath)
      case proto.WriteOperation.SaveTypeCase.TABLE =>
        val tableName = writeOperation.getTable.getTableName
        writeOperation.getTable.getSaveMethod match {
          case proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE =>
            w.saveAsTable(tableName)
          case proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO =>
            w.insertInto(tableName)
          case _ =>
            throw new UnsupportedOperationException(
              "WriteOperation:SaveTable:TableSaveMethod not supported "
                + s"${writeOperation.getTable.getSaveMethodValue}")
        }
      case _ =>
        throw new UnsupportedOperationException(
          "WriteOperation:SaveTypeCase not supported "
            + s"${writeOperation.getSaveTypeCase.getNumber}")
    }
  }

  /**
   * Transforms the write operation and executes it.
   *
   * The input write operation contains a reference to the input plan and transforms it to the
   * corresponding logical plan. Afterwards, creates the DataFrameWriter and translates the
   * parameters of the WriteOperation into the corresponding methods calls.
   *
   * @param writeOperation
   */
  private def handleWriteOperationV2(writeOperation: proto.WriteOperationV2): Unit = {
    // Transform the input plan into the logical plan.
    val plan = planner.transformRelation(writeOperation.getInput)
    // And create a Dataset from the plan.
    val dataset = Dataset.ofRows(session, logicalPlan = plan)

    val w = dataset.writeTo(table = writeOperation.getTableName)

    if (writeOperation.getOptionsCount > 0) {
      writeOperation.getOptionsMap.asScala.foreach { case (key, value) => w.option(key, value) }
    }

    if (writeOperation.getTablePropertiesCount > 0) {
      writeOperation.getTablePropertiesMap.asScala.foreach { case (key, value) =>
        w.tableProperty(key, value)
      }
    }

    if (writeOperation.getPartitioningColumnsCount > 0) {
      val names = writeOperation.getPartitioningColumnsList.asScala
        .map(planner.transformExpression)
        .map(Column(_))
        .toSeq
      w.partitionedBy(names.head, names.tail.toSeq: _*)
    }

    writeOperation.getMode match {
      case proto.WriteOperationV2.Mode.MODE_CREATE =>
        if (writeOperation.hasProvider) {
          w.using(writeOperation.getProvider).create()
        } else {
          w.create()
        }
      case proto.WriteOperationV2.Mode.MODE_OVERWRITE =>
        w.overwrite(Column(planner.transformExpression(writeOperation.getOverwriteCondition)))
      case proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS =>
        w.overwritePartitions()
      case proto.WriteOperationV2.Mode.MODE_APPEND =>
        w.append()
      case proto.WriteOperationV2.Mode.MODE_REPLACE =>
        if (writeOperation.hasProvider) {
          w.using(writeOperation.getProvider).replace()
        } else {
          w.replace()
        }
      case proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE =>
        if (writeOperation.hasProvider) {
          w.using(writeOperation.getProvider).createOrReplace()
        } else {
          w.createOrReplace()
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"WriteOperationV2:ModeValue not supported ${writeOperation.getModeValue}")
    }
  }

  private def handleWriteStreamOperationStart(
      writeOp: proto.WriteStreamOperationStart,
      sessionId: String,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    val plan = planner.transformRelation(writeOp.getInput)
    val dataset = Dataset.ofRows(session, logicalPlan = plan)

    val writer = dataset.writeStream

    if (writeOp.getFormat.nonEmpty) {
      writer.format(writeOp.getFormat)
    }

    writer.options(writeOp.getOptionsMap)

    if (writeOp.getPartitioningColumnNamesCount > 0) {
      writer.partitionBy(writeOp.getPartitioningColumnNamesList.asScala.toList: _*)
    }

    writeOp.getTriggerCase match {
      case proto.WriteStreamOperationStart.TriggerCase.PROCESSING_TIME_INTERVAL =>
        writer.trigger(Trigger.ProcessingTime(writeOp.getProcessingTimeInterval))
      case proto.WriteStreamOperationStart.TriggerCase.AVAILABLE_NOW =>
        writer.trigger(Trigger.AvailableNow())
      case proto.WriteStreamOperationStart.TriggerCase.ONCE =>
        writer.trigger(Trigger.Once())
      case proto.WriteStreamOperationStart.TriggerCase.CONTINUOUS_CHECKPOINT_INTERVAL =>
        writer.trigger(Trigger.Continuous(writeOp.getContinuousCheckpointInterval))
      case proto.WriteStreamOperationStart.TriggerCase.TRIGGER_NOT_SET =>
    }

    if (writeOp.getOutputMode.nonEmpty) {
      writer.outputMode(writeOp.getOutputMode)
    }

    if (writeOp.getQueryName.nonEmpty) {
      writer.queryName(writeOp.getQueryName)
    }

    val query = writeOp.getPath match {
      case "" if writeOp.hasTableName => writer.toTable(writeOp.getTableName)
      case "" => writer.start()
      case path => writer.start(path)
    }

    val result = proto.WriteStreamOperationStartResult
      .newBuilder()
      .setQueryId(
        proto.StreamingQueryInstanceId
          .newBuilder()
          .setId(query.id.toString)
          .setRunId(query.runId.toString)
          .build())
      .setName(Option(query.name).getOrElse(""))
      .build()

    responseObserver.onNext(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setWriteStreamOperationStartResult(result)
        .build())
  }

  private def handleStreamingQueryCommand(
      command: proto.StreamingQueryCommand,
      sessionId: String,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {

    val id = command.getQueryId.getId
    val runId = command.getQueryId.getRunId

    val respBuilder = proto.StreamingQueryCommandResult
      .newBuilder()
      .setQueryId(command.getQueryId)

    val query = Option(session.streams.get(id)) match {
      case Some(query) if query.runId.toString == runId =>
        query
      case Some(query) =>
        throw new IllegalArgumentException(
          s"Run id mismatch for query id $id. Run id in the request $runId " +
            s"does not match one on the server ${query.runId}. The query might have restarted.")
      case None =>
        throw new IllegalArgumentException(s"Streaming query $id is not found")
      // TODO(SPARK-42962): Handle this better. May be cache stopped queries for a few minutes.
    }

    command.getCommandCase match {
      case proto.StreamingQueryCommand.CommandCase.STATUS =>
        val queryStatus = query.status

        val statusResult = proto.StreamingQueryCommandResult.StatusResult
          .newBuilder()
          .setStatusMessage(queryStatus.message)
          .setIsDataAvailable(queryStatus.isDataAvailable)
          .setIsTriggerActive(queryStatus.isTriggerActive)
          .setIsActive(query.isActive)
          .build()

        respBuilder.setStatus(statusResult)

      case proto.StreamingQueryCommand.CommandCase.LAST_PROGRESS |
          proto.StreamingQueryCommand.CommandCase.RECENT_PROGRESS =>
        val progressReports = if (command.getLastProgress) {
          Option(query.lastProgress).toSeq
        } else {
          query.recentProgress.toSeq
        }
        respBuilder.setRecentProgress(
          proto.StreamingQueryCommandResult.RecentProgressResult
            .newBuilder()
            .addAllRecentProgressJson(progressReports.map(_.json).asJava)
            .build())

      case proto.StreamingQueryCommand.CommandCase.STOP =>
        query.stop()

      case proto.StreamingQueryCommand.CommandCase.PROCESS_ALL_AVAILABLE =>
        // This might take a long time, Spark-connect client keeps this connection alive.
        // TODO(SPARK-42962): Improve this as part of session management.
        query.processAllAvailable()

      case proto.StreamingQueryCommand.CommandCase.EXPLAIN =>
        val result = query match {
          case q: StreamingQueryWrapper =>
            q.streamingQuery.explainInternal(command.getExplain.getExtended)
          case _ =>
            throw new IllegalStateException(s"Unexpected type for streaming query: $query")
        }
        val explain = proto.StreamingQueryCommandResult.ExplainResult
          .newBuilder()
          .setResult(result)
          .build()
        respBuilder.setExplain(explain)

      case proto.StreamingQueryCommand.CommandCase.EXCEPTION =>
        val result = query.exception
        result.foreach(e =>
          respBuilder.getExceptionBuilder
            .setExceptionMessage(SparkConnectService.extractErrorMessage(e)))

      case proto.StreamingQueryCommand.CommandCase.AWAIT_TERMINATION =>
        if (command.getAwaitTermination.hasTimeoutMs) {
          val terminated = query.awaitTermination(command.getAwaitTermination.getTimeoutMs)
          respBuilder.getAwaitTerminationBuilder
            .setTerminated(terminated)
        } else {
          query.awaitTermination()
          respBuilder.getAwaitTerminationBuilder
            .setTerminated(true)
        }

      case proto.StreamingQueryCommand.CommandCase.COMMAND_NOT_SET =>
        throw new IllegalArgumentException("Missing command in StreamingQueryCommand")
    }

    responseObserver.onNext(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setStreamingQueryCommandResult(respBuilder.build())
        .build())
  }

  private def handleGetResourcesCommand(
      command: proto.GetResourcesCommand,
      sessionId: String,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    responseObserver.onNext(
      proto.ExecutePlanResponse
        .newBuilder()
        .setSessionId(sessionId)
        .setGetResourcesCommandResult(
          proto.GetResourcesCommandResult
            .newBuilder()
            .putAllResources(
              session.sparkContext.resources
                .mapValues(resource =>
                  proto.ResourceInformation
                    .newBuilder()
                    .setName(resource.name)
                    .addAllAddresses(resource.addresses.toIterable.asJava)
                    .build())
                .toMap
                .asJava)
            .build())
        .build())
  }
}
