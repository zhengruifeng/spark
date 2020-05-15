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

package org.apache.spark.ml.linalg

import scala.util.Random

import org.apache.spark.ml.SparkMLFunSuite
import org.apache.spark.ml.linalg.BLAS._
import org.apache.spark.ml.util.TestingUtils._

class BLASSuite extends SparkMLFunSuite {

  test("performance: gemv vs dot") {
    for (numRows <- Seq(16, 64, 256, 1024, 4096); numCols <- Seq(16, 64, 256, 1024, 4096)) {
      val rng = new Random(123)
      val matrix = Matrices.dense(numRows, numCols,
        Array.fill(numRows * numCols)(rng.nextDouble)).toDense
      val vectors = matrix.rowIter.toArray
      val vector = Vectors.dense(Array.fill(numCols)(rng.nextDouble))

      val start1 = System.nanoTime
      Seq.range(0, 100).foreach { _ => matrix.multiply(vector) }
      val dur1 = System.nanoTime - start1

      val start2 = System.nanoTime
      Seq.range(0, 100).foreach { _ => vectors.map(vector.dot) }
      val dur2 = System.nanoTime - start2

      println(s"numRows=$numRows, numCols=$numCols, gemv: $dur1, dot: $dur2, " +
        s"dot/gemv: ${dur2.toDouble / dur1}")
    }
  }

  test("performance: gemv vs foreachNonZero") {
    for (numRows <- Seq(16, 64, 256, 1024, 4096); numCols <- Seq(16, 64, 256, 1024, 4096)) {
      val rng = new Random(123)
      val matrix = Matrices.dense(numRows, numCols,
        Array.fill(numRows * numCols)(rng.nextDouble)).toDense
      val vectors = matrix.rowIter.toArray
      val coefVec = Vectors.dense(Array.fill(numCols)(rng.nextDouble))
      val coefArr = coefVec.toArray

      val start1 = System.nanoTime
      Seq.range(0, 100).foreach { _ => matrix.multiply(coefVec) }
      val dur1 = System.nanoTime - start1

      val start2 = System.nanoTime
      Seq.range(0, 100).foreach { _ =>
        vectors.map { vector =>
          var sum = 0.0
          vector.foreachNonZero((i, v) => sum += coefArr(i) * v)
          sum
        }
      }
      val dur2 = System.nanoTime - start2

      println(s"numRows=$numRows, numCols=$numCols, gemv: $dur1, foreachNonZero: $dur2, " +
        s"foreachNonZero/gemv: ${dur2.toDouble / dur1}")
    }
  }

  test("performance: gemv vs foreachNonZero(std)") {
    for (numRows <- Seq(16, 64, 256, 1024, 4096); numCols <- Seq(16, 64, 256, 1024, 4096)) {
      val rng = new Random(123)
      val matrix = Matrices.dense(numRows, numCols,
        Array.fill(numRows * numCols)(rng.nextDouble)).toDense
      val vectors = matrix.rowIter.toArray
      val coefVec = Vectors.dense(Array.fill(numCols)(rng.nextDouble))
      val coefArr = coefVec.toArray
      val stdVec = Vectors.dense(Array.fill(numCols)(rng.nextDouble))
      val stdArr = stdVec.toArray

      val start1 = System.nanoTime
      Seq.range(0, 100).foreach { _ => matrix.multiply(coefVec) }
      val dur1 = System.nanoTime - start1

      val start2 = System.nanoTime
      Seq.range(0, 100).foreach { _ =>
        vectors.map { vector =>
          var sum = 0.0
          vector.foreachNonZero { (i, v) =>
            val std = stdArr(i)
            if (std != 0) sum += coefArr(i) * v
          }
          sum
        }
      }
      val dur2 = System.nanoTime - start2

      println(s"numRows=$numRows, numCols=$numCols, gemv: $dur1, foreachNonZero(std): $dur2, " +
        s"foreachNonZero(std)/gemv: ${dur2.toDouble / dur1}")
    }
  }

  test("performance: gemv vs while") {
    for (numRows <- Seq(16, 64, 256, 1024, 4096); numCols <- Seq(16, 64, 256, 1024, 4096)) {
      val rng = new Random(123)
      val matrix = Matrices.dense(numRows, numCols,
        Array.fill(numRows * numCols)(rng.nextDouble)).toDense
      val vectors = matrix.rowIter.toArray
      val coefVec = Vectors.dense(Array.fill(numCols)(rng.nextDouble))
      val coefArr = coefVec.toArray

      val start1 = System.nanoTime
      Seq.range(0, 100).foreach { _ => matrix.multiply(coefVec) }
      val dur1 = System.nanoTime - start1

      val start2 = System.nanoTime
      Seq.range(0, 100).foreach { _ =>
        vectors.map {
          case DenseVector(values) =>
            var sum = 0.0
            var i = 0
            while (i < values.length) {
              sum += values(i) * coefArr(i)
              i += 1
            }
            sum
        }
      }
      val dur2 = System.nanoTime - start2

      println(s"numRows=$numRows, numCols=$numCols, gemv: $dur1, while: $dur2, " +
        s"while/gemv: ${dur2.toDouble / dur1}")
    }
  }

  test("performance: gemv vs while(std)") {
    for (numRows <- Seq(16, 64, 256, 1024, 4096); numCols <- Seq(16, 64, 256, 1024, 4096)) {
      val rng = new Random(123)
      val matrix = Matrices.dense(numRows, numCols,
        Array.fill(numRows * numCols)(rng.nextDouble)).toDense
      val vectors = matrix.rowIter.toArray
      val coefVec = Vectors.dense(Array.fill(numCols)(rng.nextDouble))
      val coefArr = coefVec.toArray
      val stdVec = Vectors.dense(Array.fill(numCols)(rng.nextDouble))
      val stdArr = stdVec.toArray

      val start1 = System.nanoTime
      Seq.range(0, 100).foreach { _ => matrix.multiply(coefVec) }
      val dur1 = System.nanoTime - start1

      val start2 = System.nanoTime
      Seq.range(0, 100).foreach { _ =>
        vectors.map {
          case DenseVector(values) =>
            var sum = 0.0
            var i = 0
            while (i < values.length) {
              val std = stdArr(i)
              if (std != 0) sum += values(i) * coefArr(i) / std
              i += 1
            }
            sum
        }
      }
      val dur2 = System.nanoTime - start2

      println(s"numRows=$numRows, numCols=$numCols, gemv: $dur1, while(std): $dur2, " +
        s"while(std)/gemv: ${dur2.toDouble / dur1}")
    }
  }


  test("performance: gemm vs gemv") {
    for (numRows <- Seq(16, 64, 256, 1024); numCols <- Seq(16, 64, 256, 1024)) {
      val rng = new Random(123)
      val matrix = Matrices.dense(numRows, numCols,
        Array.fill(numRows * numCols)(rng.nextDouble)).toDense
      val matrix2 = Matrices.dense(numCols, numRows,
        Array.fill(numRows * numCols)(rng.nextDouble)).toDense
      val vectors = matrix2.colIter.toArray

      val start1 = System.nanoTime
      Seq.range(0, 10).foreach { _ => matrix.multiply(matrix2) }
      val dur1 = System.nanoTime - start1

      val start2 = System.nanoTime
      Seq.range(0, 10).foreach { _ => vectors.map(matrix.multiply) }
      val dur2 = System.nanoTime - start2

      println(s"numRows=$numRows, numCols=$numCols, gemm: $dur1, gemv: $dur2," +
        s" gemv/gemm: ${dur2.toDouble / dur1}")
    }
  }
}
