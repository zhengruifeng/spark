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

package org.apache.spark.ml.util

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

// A helper Transformer only used to:
// 1, fail model saving when shouldOverwrite=false and the path already exists;
// 2, clean the path when shouldOverwrite=true;
private[ml] class PathCleaner(override val uid: String) extends Transformer with MLWritable {

  def this() = this(Identifiable.randomUID("pathCleaner"))

  override def transform(dataset: Dataset[_]): DataFrame =
    throw new UnsupportedOperationException("PathCleaner.transform")

  override def transformSchema(schema: StructType): StructType =
    throw new UnsupportedOperationException("PathCleaner.transformSchema")

  override def copy(extra: ParamMap): PathCleaner = defaultCopy(extra)


  override def write: MLWriter = new MLWriter {
    // PathCleaner.write().overwrite().save(path) invokes
    // FileSystemOverwrite().handleOverwrite() to check and clean the path
    override protected def saveImpl(path: String): Unit = {
      // Do nothing
    }
  }
}
