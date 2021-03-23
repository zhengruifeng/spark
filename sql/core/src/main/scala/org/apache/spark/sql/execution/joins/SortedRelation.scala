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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.util.{CompletionIterator, KnownSizeEstimation, Utils}


/**
 * Interface for a sorted relation by some key. Use [[SortedRelation.apply]] to create a concrete
 * object.
 */
sealed trait SortedRelation extends KnownSizeEstimation {

  /**
   * Returns true iff all the keys are unique.
   */
  def keyIsUnique: Boolean

  def getIterator: Iterator[InternalRow]

  /**
   * Release any used resources.
   */
  def close(): Unit
}


private[execution] object SortedRelation {
  def apply(
      rdd: RDD[InternalRow],
      input: Iterator[InternalRow],
      key: Seq[Expression]): SortedRelation = {
    SortedFilesRelation.apply(rdd, input, key)
  }
}


private[execution] object SortedFilesRelation extends Logging {
  /**
   * Create a SortedRelation from an RDD of InternalRow.
   */
  def apply(
      rdd: RDD[InternalRow],
      input: Iterator[InternalRow],
      key: Seq[Expression]): SortedFilesRelation = {
    val sparkContext = rdd.sparkContext

    val buildFile = s"${sparkContext.applicationId}_bsj_build_file_${UUID.randomUUID}"
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
    val buildIter = rdd.map(_.copy()).toLocalIterator
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
    new SortedFilesRelation(Array(buildFile), false, 1L)
  }

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


private[joins] class SortedFilesRelation(
    val files: Array[String],
    val keyIsUnique: Boolean,
    val estimatedSize: Long) extends SortedRelation {

  override def getIterator: Iterator[InternalRow] = {
    SortedFilesRelation.createInternalRowIterator(files.map(SparkFiles.get))
  }

  // TODO: remove files from Spark File Server.
  override def close(): Unit = { }
}


/** The SortedRelationBroadcastMode requires that rows are stored in files
 *  and then broadcasted as a SortedRelation.
 */
case class SortedRelationBroadcastMode(key: Seq[Expression])
  extends BroadcastMode {

  override def transform(rows: Array[InternalRow]): SortedRelation = {
    null
  }


  override lazy val canonicalized: SortedRelationBroadcastMode = {
    this.copy(key = key.map(_.canonicalized))
  }

  override def transform(rows: Iterator[InternalRow], sizeHint: Option[Long]): Any = null
}
