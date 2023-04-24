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

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.connect.common.{InvalidPlanInput, StorageLevelProtoConverter}
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.internal.CatalogImpl
import org.apache.spark.sql.types._

private[connect] class SparkConnectCatalogHandler(planner: SparkConnectPlanner) extends Logging {
  import SparkConnectCatalogHandler.emptyLocalRelation

  def this(session: SparkSession) = {
    this(new SparkConnectPlanner(session))
  }

  private def session = planner.session

  def handle(catalog: proto.Catalog): logical.LogicalPlan = {
    catalog.getCatTypeCase match {
      case proto.Catalog.CatTypeCase.CURRENT_DATABASE =>
        transformCurrentDatabase(catalog.getCurrentDatabase)
      case proto.Catalog.CatTypeCase.SET_CURRENT_DATABASE =>
        transformSetCurrentDatabase(catalog.getSetCurrentDatabase)
      case proto.Catalog.CatTypeCase.LIST_DATABASES =>
        transformListDatabases(catalog.getListDatabases)
      case proto.Catalog.CatTypeCase.LIST_TABLES => transformListTables(catalog.getListTables)
      case proto.Catalog.CatTypeCase.LIST_FUNCTIONS =>
        transformListFunctions(catalog.getListFunctions)
      case proto.Catalog.CatTypeCase.LIST_COLUMNS => transformListColumns(catalog.getListColumns)
      case proto.Catalog.CatTypeCase.GET_DATABASE => transformGetDatabase(catalog.getGetDatabase)
      case proto.Catalog.CatTypeCase.GET_TABLE => transformGetTable(catalog.getGetTable)
      case proto.Catalog.CatTypeCase.GET_FUNCTION => transformGetFunction(catalog.getGetFunction)
      case proto.Catalog.CatTypeCase.DATABASE_EXISTS =>
        transformDatabaseExists(catalog.getDatabaseExists)
      case proto.Catalog.CatTypeCase.TABLE_EXISTS => transformTableExists(catalog.getTableExists)
      case proto.Catalog.CatTypeCase.FUNCTION_EXISTS =>
        transformFunctionExists(catalog.getFunctionExists)
      case proto.Catalog.CatTypeCase.CREATE_EXTERNAL_TABLE =>
        transformCreateExternalTable(catalog.getCreateExternalTable)
      case proto.Catalog.CatTypeCase.CREATE_TABLE => transformCreateTable(catalog.getCreateTable)
      case proto.Catalog.CatTypeCase.DROP_TEMP_VIEW =>
        transformDropTempView(catalog.getDropTempView)
      case proto.Catalog.CatTypeCase.DROP_GLOBAL_TEMP_VIEW =>
        transformDropGlobalTempView(catalog.getDropGlobalTempView)
      case proto.Catalog.CatTypeCase.RECOVER_PARTITIONS =>
        transformRecoverPartitions(catalog.getRecoverPartitions)
      case proto.Catalog.CatTypeCase.IS_CACHED => transformIsCached(catalog.getIsCached)
      case proto.Catalog.CatTypeCase.CACHE_TABLE => transformCacheTable(catalog.getCacheTable)
      case proto.Catalog.CatTypeCase.UNCACHE_TABLE =>
        transformUncacheTable(catalog.getUncacheTable)
      case proto.Catalog.CatTypeCase.CLEAR_CACHE => transformClearCache(catalog.getClearCache)
      case proto.Catalog.CatTypeCase.REFRESH_TABLE =>
        transformRefreshTable(catalog.getRefreshTable)
      case proto.Catalog.CatTypeCase.REFRESH_BY_PATH =>
        transformRefreshByPath(catalog.getRefreshByPath)
      case proto.Catalog.CatTypeCase.CURRENT_CATALOG =>
        transformCurrentCatalog(catalog.getCurrentCatalog)
      case proto.Catalog.CatTypeCase.SET_CURRENT_CATALOG =>
        transformSetCurrentCatalog(catalog.getSetCurrentCatalog)
      case proto.Catalog.CatTypeCase.LIST_CATALOGS =>
        transformListCatalogs(catalog.getListCatalogs)
      case other => throw InvalidPlanInput(s"$other not supported.")
    }
  }

  private def transformCurrentDatabase(getCurrentDatabase: proto.CurrentDatabase) = {
    session.createDataset(session.catalog.currentDatabase :: Nil)(Encoders.STRING).logicalPlan
  }

  private def transformSetCurrentDatabase(getSetCurrentDatabase: proto.SetCurrentDatabase) = {
    session.catalog.setCurrentDatabase(getSetCurrentDatabase.getDbName)
    emptyLocalRelation
  }

  private def transformListDatabases(getListDatabases: proto.ListDatabases) = {
    session.catalog.listDatabases().logicalPlan
  }

  private def transformListTables(getListTables: proto.ListTables) = {
    if (getListTables.hasDbName) {
      session.catalog.listTables(getListTables.getDbName).logicalPlan
    } else {
      session.catalog.listTables().logicalPlan
    }
  }

  private def transformListFunctions(getListFunctions: proto.ListFunctions) = {
    if (getListFunctions.hasDbName) {
      session.catalog.listFunctions(getListFunctions.getDbName).logicalPlan
    } else {
      session.catalog.listFunctions().logicalPlan
    }
  }

  private def transformListColumns(getListColumns: proto.ListColumns) = {
    if (getListColumns.hasDbName) {
      session.catalog
        .listColumns(dbName = getListColumns.getDbName, tableName = getListColumns.getTableName)
        .logicalPlan
    } else {
      session.catalog.listColumns(getListColumns.getTableName).logicalPlan
    }
  }

  private def transformGetDatabase(getGetDatabase: proto.GetDatabase) = {
    CatalogImpl
      .makeDataset(session.catalog.getDatabase(getGetDatabase.getDbName) :: Nil, session)
      .logicalPlan
  }

  private def transformGetTable(getGetTable: proto.GetTable) = {
    if (getGetTable.hasDbName) {
      CatalogImpl
        .makeDataset(
          session.catalog.getTable(
            dbName = getGetTable.getDbName,
            tableName = getGetTable.getTableName) :: Nil,
          session)
        .logicalPlan
    } else {
      CatalogImpl
        .makeDataset(session.catalog.getTable(getGetTable.getTableName) :: Nil, session)
        .logicalPlan
    }
  }

  private def transformGetFunction(getGetFunction: proto.GetFunction) = {
    if (getGetFunction.hasDbName) {
      CatalogImpl
        .makeDataset(
          session.catalog.getFunction(
            dbName = getGetFunction.getDbName,
            functionName = getGetFunction.getFunctionName) :: Nil,
          session)
        .logicalPlan
    } else {
      CatalogImpl
        .makeDataset(session.catalog.getFunction(getGetFunction.getFunctionName) :: Nil, session)
        .logicalPlan
    }
  }

  private def transformDatabaseExists(getDatabaseExists: proto.DatabaseExists) = {
    session
      .createDataset(session.catalog.databaseExists(getDatabaseExists.getDbName) :: Nil)(
        Encoders.scalaBoolean)
      .logicalPlan
  }

  private def transformTableExists(getTableExists: proto.TableExists) = {
    if (getTableExists.hasDbName) {
      session
        .createDataset(
          session.catalog.tableExists(
            dbName = getTableExists.getDbName,
            tableName = getTableExists.getTableName) :: Nil)(Encoders.scalaBoolean)
        .logicalPlan
    } else {
      session
        .createDataset(session.catalog.tableExists(getTableExists.getTableName) :: Nil)(
          Encoders.scalaBoolean)
        .logicalPlan
    }
  }

  private def transformFunctionExists(getFunctionExists: proto.FunctionExists) = {
    if (getFunctionExists.hasDbName) {
      session
        .createDataset(
          session.catalog.functionExists(
            dbName = getFunctionExists.getDbName,
            functionName = getFunctionExists.getFunctionName) :: Nil)(Encoders.scalaBoolean)
        .logicalPlan
    } else {
      session
        .createDataset(session.catalog.functionExists(getFunctionExists.getFunctionName) :: Nil)(
          Encoders.scalaBoolean)
        .logicalPlan
    }
  }

  private def transformCreateExternalTable(getCreateExternalTable: proto.CreateExternalTable) = {
    val schema = if (getCreateExternalTable.hasSchema) {
      val struct = planner.transformDataType(getCreateExternalTable.getSchema)
      assert(struct.isInstanceOf[StructType])
      struct.asInstanceOf[StructType]
    } else {
      new StructType
    }

    val source = if (getCreateExternalTable.hasSource) {
      getCreateExternalTable.getSource
    } else {
      session.sessionState.conf.defaultDataSourceName
    }

    val options = if (getCreateExternalTable.hasPath) {
      (getCreateExternalTable.getOptionsMap.asScala ++
        Map("path" -> getCreateExternalTable.getPath)).asJava
    } else {
      getCreateExternalTable.getOptionsMap
    }
    session.catalog
      .createTable(
        tableName = getCreateExternalTable.getTableName,
        source = source,
        schema = schema,
        options = options)
      .logicalPlan
  }

  private def transformCreateTable(getCreateTable: proto.CreateTable) = {
    val schema = if (getCreateTable.hasSchema) {
      val struct = planner.transformDataType(getCreateTable.getSchema)
      assert(struct.isInstanceOf[StructType])
      struct.asInstanceOf[StructType]
    } else {
      new StructType
    }

    val source = if (getCreateTable.hasSource) {
      getCreateTable.getSource
    } else {
      session.sessionState.conf.defaultDataSourceName
    }

    val description = if (getCreateTable.hasDescription) {
      getCreateTable.getDescription
    } else {
      ""
    }

    val options = if (getCreateTable.hasPath) {
      (getCreateTable.getOptionsMap.asScala ++
        Map("path" -> getCreateTable.getPath)).asJava
    } else {
      getCreateTable.getOptionsMap
    }

    session.catalog
      .createTable(
        tableName = getCreateTable.getTableName,
        source = source,
        schema = schema,
        description = description,
        options = options)
      .logicalPlan
  }

  private def transformDropTempView(getDropTempView: proto.DropTempView) = {
    session
      .createDataset(session.catalog.dropTempView(getDropTempView.getViewName) :: Nil)(
        Encoders.scalaBoolean)
      .logicalPlan
  }

  private def transformDropGlobalTempView(getDropGlobalTempView: proto.DropGlobalTempView) = {
    session
      .createDataset(
        session.catalog.dropGlobalTempView(getDropGlobalTempView.getViewName) :: Nil)(
        Encoders.scalaBoolean)
      .logicalPlan
  }

  private def transformRecoverPartitions(getRecoverPartitions: proto.RecoverPartitions) = {
    session.catalog.recoverPartitions(getRecoverPartitions.getTableName)
    emptyLocalRelation
  }

  private def transformIsCached(getIsCached: proto.IsCached) = {
    session
      .createDataset(session.catalog.isCached(getIsCached.getTableName) :: Nil)(
        Encoders.scalaBoolean)
      .logicalPlan
  }

  private def transformCacheTable(getCacheTable: proto.CacheTable) = {
    if (getCacheTable.hasStorageLevel) {
      session.catalog.cacheTable(
        getCacheTable.getTableName,
        StorageLevelProtoConverter.toStorageLevel(getCacheTable.getStorageLevel))
    } else {
      session.catalog.cacheTable(getCacheTable.getTableName)
    }
    emptyLocalRelation
  }

  private def transformUncacheTable(getUncacheTable: proto.UncacheTable) = {
    session.catalog.uncacheTable(getUncacheTable.getTableName)
    emptyLocalRelation
  }

  private def transformClearCache(getClearCache: proto.ClearCache) = {
    session.catalog.clearCache()
    emptyLocalRelation
  }

  private def transformRefreshTable(getRefreshTable: proto.RefreshTable) = {
    session.catalog.refreshTable(getRefreshTable.getTableName)
    emptyLocalRelation
  }

  private def transformRefreshByPath(getRefreshByPath: proto.RefreshByPath) = {
    session.catalog.refreshByPath(getRefreshByPath.getPath)
    emptyLocalRelation
  }

  private def transformCurrentCatalog(getCurrentCatalog: proto.CurrentCatalog) = {
    session.createDataset(session.catalog.currentCatalog() :: Nil)(Encoders.STRING).logicalPlan
  }

  private def transformSetCurrentCatalog(getSetCurrentCatalog: proto.SetCurrentCatalog) = {
    session.catalog.setCurrentCatalog(getSetCurrentCatalog.getCatalogName)
    emptyLocalRelation
  }

  private def transformListCatalogs(getListCatalogs: proto.ListCatalogs) = {
    session.catalog.listCatalogs().logicalPlan
  }
}

private[connect] object SparkConnectCatalogHandler {

  val emptyLocalRelation = logical.LocalRelation(
    output = expressions.AttributeReference("value", StringType, false)() :: Nil,
    data = Seq.empty)
}
