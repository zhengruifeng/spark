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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, NarrowDependency, Partition, TaskContext}


private[spark] case class PartitionRecombinedRDDPartition(
    index: Int,
    @transient rdd: RDD[_],
    parentsIndices: Array[Int])
  extends Partition with Serializable {
  val parents: Seq[Partition] = parentsIndices.map(rdd.partitions)
}


private[spark] class PartitionRecombinedRDD[T: ClassTag](
    var prev: RDD[T],
    val recombinedIndices: Array[Array[Int]])
  extends RDD[T](prev.context, Nil) {  // Nil since we implement getDependencies

  private val numParentPartitions = prev.getNumPartitions

  recombinedIndices.foreach { parentsIndices =>
    require(parentsIndices != null, "Invalid partition indices: NULL")
    require(parentsIndices.forall(index => 0 <= index && index < numParentPartitions),
      s"Invalid partition indices ${parentsIndices.mkString("[", ",", "]")}")
  }

  override protected def getPartitions: Array[Partition] = {
    recombinedIndices.zipWithIndex.map { case (parentsIndices, index) =>
      PartitionRecombinedRDDPartition(index, prev, parentsIndices)
    }
  }

  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    partition.asInstanceOf[PartitionRecombinedRDDPartition].parents.iterator
      .flatMap { parentPartition => firstParent[T].iterator(parentPartition, context) }
  }

  override def getDependencies: Seq[Dependency[_]] = {
    new NarrowDependency(prev) {
      def getParents(id: Int): Seq[Int] =
        partitions(id).asInstanceOf[PartitionRecombinedRDDPartition]
          .parentsIndices.distinct.sorted
    } :: Nil
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
}
