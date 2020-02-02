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

package org.apache.spark.ml.clustering

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.{Estimator, Model, PipelineStage}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.clustering.{KMeans => OldKMeans, KMeansModel => OldKMeansModel}
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.VersionUtils.majorVersion

/**
 * Common params for KMeans and KMeansModel
 */
private[clustering] trait KMeansParams extends Params with HasMaxIter with HasFeaturesCol
  with HasSeed with HasPredictionCol with HasTol with HasDistanceMeasure with HasWeightCol
  with HasBlockSize {

  /**
   * The number of clusters to create (k). Must be &gt; 1. Note that it is possible for fewer than
   * k clusters to be returned, for example, if there are fewer than k distinct points to cluster.
   * Default: 2.
   * @group param
   */
  @Since("1.5.0")
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("1.5.0")
  def getK: Int = $(k)

  /**
   * Param for the initialization algorithm. This can be either "random" to choose random points as
   * initial cluster centers, or "k-means||" to use a parallel variant of k-means++
   * (Bahmani et al., Scalable K-Means++, VLDB 2012). Default: k-means||.
   * @group expertParam
   */
  @Since("1.5.0")
  final val initMode = new Param[String](this, "initMode", "The initialization algorithm. " +
    "Supported options: 'random' and 'k-means||'.",
    ParamValidators.inArray[String](KMeans.supportedInitModes))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitMode: String = $(initMode)

  /**
   * Param for the number of steps for the k-means|| initialization mode. This is an advanced
   * setting -- the default of 2 is almost always enough. Must be &gt; 0. Default: 2.
   * @group expertParam
   */
  @Since("1.5.0")
  final val initSteps = new IntParam(this, "initSteps", "The number of steps for k-means|| " +
    "initialization mode. Must be > 0.", ParamValidators.gt(0))

  /** @group expertGetParam */
  @Since("1.5.0")
  def getInitSteps: Int = $(initSteps)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.validateVectorCompatibleColumn(schema, getFeaturesCol)
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
  }
}

/**
 * Model fitted by KMeans.
 *
 * @param centerMatrix centers of clusters, of size K X numFeatures.
 */
@Since("1.5.0")
class KMeansModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("3.1.0") val centerMatrix: Matrix)
  extends Model[KMeansModel] with KMeansParams with GeneralMLWritable
    with HasTrainingSummary[KMeansSummary] {
  import KMeans.{EUCLIDEAN, COSINE}

  @transient private lazy val negSquaredNormsVec = {
    $(distanceMeasure) match {
      case EUCLIDEAN =>
        val negSquaredNorms = centerMatrix.rowIter.map(center => - BLAS.dot(center, center))
        new DenseVector(negSquaredNorms.toArray)
      case COSINE => null
    }
  }

  @Since("3.0.0")
  lazy val numFeatures: Int = centerMatrix.numCols

  @Since("1.5.0")
  override def copy(extra: ParamMap): KMeansModel = {
    val copied = copyValues(new KMeansModel(uid, centerMatrix.copy), extra)
    copied.setSummary(trainingSummary).setParent(this.parent)
  }

  /** @group setParam */
  @Since("2.0.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.0.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val predictUDF = udf((vector: Vector) => predict(vector))

    dataset.withColumn($(predictionCol),
      predictUDF(DatasetUtils.columnToVector(dataset, getFeaturesCol)),
      outputSchema($(predictionCol)).metadata)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(predictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateNumValues(outputSchema,
        $(predictionCol), centerMatrix.numRows)
    }
    outputSchema
  }

  @transient private lazy val predictFunc: Vector => Int = {
    $(distanceMeasure) match {
      case EUCLIDEAN =>
        (vec: Vector) =>
          val negSquared = negSquaredNormsVec.copy
          BLAS.gemv(2.0, centerMatrix, vec, 1.0, negSquared)
          negSquared.argmax
      case COSINE =>
        (vec: Vector) =>
          val dots = new DenseVector(Array.ofDim[Double](centerMatrix.numRows))
          BLAS.gemv(1.0, centerMatrix, vec, 0.0, dots)
          dots.argmax
    }
  }

  @Since("3.0.0")
  def predict(features: Vector): Int = predictFunc(features)

  @Since("2.0.0")
  def clusterCenters: Array[Vector] = centerMatrix.rowIter.toArray

  /**
   * Returns a [[org.apache.spark.ml.util.GeneralMLWriter]] instance for this ML instance.
   *
   * For [[KMeansModel]], this does NOT currently save the training [[summary]].
   * An option to save [[summary]] may be added in the future.
   *
   */
  @Since("1.6.0")
  override def write: GeneralMLWriter = new GeneralMLWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"KMeansModel: uid=$uid, k=${centerMatrix.numRows}, distanceMeasure=${$(distanceMeasure)}, " +
      s"numFeatures=$numFeatures"
  }

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `hasSummary` is false.
   */
  @Since("2.0.0")
  override def summary: KMeansSummary = super.summary
}

/** Helper class for storing model data */
private case class ClusterData(clusterIdx: Int, clusterCenter: Vector)


/** A writer for KMeans that handles the "internal" (or default) format */
private class InternalKMeansModelWriter extends MLWriterFormat with MLFormatRegister {

  override def format(): String = "internal"
  override def stageName(): String = "org.apache.spark.ml.clustering.KMeansModel"

  override def write(path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    val instance = stage.asInstanceOf[KMeansModel]
    val sc = sparkSession.sparkContext
    // Save metadata and Params
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    // Save model data: cluster centers
    val data: Array[ClusterData] = instance.clusterCenters.zipWithIndex.map {
      case (center, idx) =>
        ClusterData(idx, center)
    }
    val dataPath = new Path(path, "data").toString
    sparkSession.createDataFrame(data).repartition(1).write.parquet(dataPath)
  }
}

/** A writer for KMeans that handles the "pmml" format */
private class PMMLKMeansModelWriter extends MLWriterFormat with MLFormatRegister {

  override def format(): String = "pmml"
  override def stageName(): String = "org.apache.spark.ml.clustering.KMeansModel"

  override def write(path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage): Unit = {
    val instance = stage.asInstanceOf[KMeansModel]
    val sc = sparkSession.sparkContext
    val oldModel = if (instance.hasSummary) {
      new OldKMeansModel(instance.clusterCenters.map(OldVectors.fromML),
        instance.getDistanceMeasure, instance.summary.trainingCost, instance.summary.numIter)
    } else {
      new OldKMeansModel(instance.clusterCenters.map(OldVectors.fromML),
        instance.getDistanceMeasure, Double.NaN, -1)
    }
    oldModel.toPMML(sc, path)
  }
}


@Since("1.6.0")
object KMeansModel extends MLReadable[KMeansModel] {

  @Since("1.6.0")
  override def read: MLReader[KMeansModel] = new KMeansModelReader

  @Since("1.6.0")
  override def load(path: String): KMeansModel = super.load(path)

  /**
   * We store all cluster centers in a single row and use this class to store model data by
   * Spark 1.6 and earlier. A model can be loaded from such older data for backward compatibility.
   */
  private case class OldData(clusterCenters: Array[OldVector])

  private class KMeansModelReader extends MLReader[KMeansModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[KMeansModel].getName

    override def load(path: String): KMeansModel = {
      // Import implicits for Dataset Encoder
      val sparkSession = super.sparkSession
      import sparkSession.implicits._

      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString

      val clusterCenters = if (majorVersion(metadata.sparkVersion) >= 2) {
        val data: Dataset[ClusterData] = sparkSession.read.parquet(dataPath).as[ClusterData]
        data.collect().sortBy(_.clusterIdx).map(_.clusterCenter)
      } else {
        // Loads KMeansModel stored with the old format used by Spark 1.6 and earlier.
        sparkSession.read.parquet(dataPath).as[OldData].head().clusterCenters.map(_.asML)
      }
      val centerMatrix = Matrices.fromVectors(clusterCenters)
      val model = new KMeansModel(metadata.uid, centerMatrix)
      metadata.getAndSetParams(model)
      model
    }
  }
}

/**
 * K-means clustering with support for k-means|| initialization proposed by Bahmani et al.
 *
 * @see <a href="https://doi.org/10.14778/2180912.2180915">Bahmani et al., Scalable k-means++.</a>
 */
@Since("1.5.0")
class KMeans @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends Estimator[KMeansModel] with KMeansParams with DefaultParamsWritable {
  import KMeans._

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> K_MEANS_PARALLEL,
    initSteps -> 2,
    tol -> 1e-4,
    distanceMeasure -> EUCLIDEAN)

  @Since("1.5.0")
  override def copy(extra: ParamMap): KMeans = defaultCopy(extra)

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("kmeans"))

  /** @group setParam */
  @Since("1.5.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group expertSetParam */
  @Since("2.4.0")
  def setDistanceMeasure(value: String): this.type = set(distanceMeasure, value)

  /** @group expertSetParam */
  @Since("1.5.0")
  def setInitSteps(value: Int): this.type = set(initSteps, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.5.0")
  def setTol(value: Double): this.type = set(tol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Set block size for stacking input data in matrices.
   * Default is 4096.
   *
   * @group expertSetParam
   */
  @Since("3.1.0")
  def setBlockSize(value: Int): this.type = set(blockSize, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): KMeansModel = instrumented { instr =>
    transformSchema(dataset.schema, logging = true)

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, featuresCol, predictionCol, k, initMode, initSteps, distanceMeasure,
      maxIter, seed, tol, weightCol, blockSize)

    val sc = dataset.sparkSession.sparkContext

    val initStartTime = System.nanoTime
    var centerMatrix = initialize(dataset)
    val initTimeInSeconds = (System.nanoTime - initStartTime) / 1e9
    logInfo(f"Initialization with ${$(initMode)} took $initTimeInSeconds%.3f seconds.")

    val numFeatures = centerMatrix.numCols
    instr.logNumFeatures(numFeatures)

    val w = if (isDefined(weightCol) && $(weightCol).nonEmpty) {
      col($(weightCol)).cast(DoubleType)
    } else {
      lit(1.0)
    }

    val instances = dataset.select(DatasetUtils.columnToVector(dataset, getFeaturesCol), w).rdd
      .map { case Row(features: Vector, weight: Double) => (features, weight) }

    val localBlockSize = $(blockSize)
    val blocks = $(distanceMeasure) match {
      case EUCLIDEAN =>
        instances.mapPartitions { iter =>
          iter.map { case (features, weight) =>
            val squaredNorms = BLAS.dot(features, features)
            (squaredNorms, weight, features)
          }.grouped(localBlockSize).map { seq =>
            (seq.map(_._1).toArray, seq.map(_._2).toArray, Matrices.fromVectors(seq.map(_._3)))
          }
        }
      case COSINE =>
        instances.mapPartitions { iter =>
          iter.map { case (features, weight) =>
            val norm = Vectors.norm(features, 2)
            require(norm > 0, "Cosine distance is not defined for zero-length vectors.")
            BLAS.scal(1.0 / norm, features)
            (weight, features)
          }.grouped(localBlockSize).map { seq =>
            (Array.emptyDoubleArray, seq.map(_._1).toArray, Matrices.fromVectors(seq.map(_._2)))
          }
        }
    }

    blocks.persist(StorageLevel.MEMORY_AND_DISK)
      .setName(s"training dataset (blockSize=${$(blockSize)})")

    var converged = false
    var cost = 0.0
    var iteration = 0

    val iterationStartTime = System.nanoTime

    // Execute iterations of Lloyd's algorithm until converged
    while (iteration < $(maxIter) && !converged) {
      // Find the new centers
      val bcCenters = sc.broadcast(centerMatrix)
      val agg = blocks.treeAggregate(new KMeansAggregator(bcCenters,
        $(k), numFeatures, $(blockSize), $(distanceMeasure)))(
        seqOp = (c, b) => c.add(b),
        combOp = (c1, c2) => c1.merge(c2),
        depth = 2)
      bcCenters.destroy()

      if (iteration == 0) {
        instr.logNumExamples(agg.count)
        instr.logSumOfWeights(agg.weightSum)
      }

      val (newCenters, distances) = agg.result

      // Update the cluster centers and costs
      converged = distances.forall(_ <= $(tol))
      cost = agg.costSum
      centerMatrix = newCenters

      iteration += 1
    }

    val iterationTimeInSeconds = (System.nanoTime() - iterationStartTime) / 1e9
    logInfo(f"Iterations took $iterationTimeInSeconds%.3f seconds.")

    if (iteration == $(maxIter)) {
      logInfo(s"KMeans reached the max number of iterations: ${$(maxIter)}.")
    } else {
      logInfo(s"KMeans converged in $iteration iterations.")
    }

    logInfo(s"The cost is $cost.")

    val model = copyValues(new KMeansModel(uid, centerMatrix).setParent(this))
    val summary = new KMeansSummary(
      model.transform(dataset),
      $(predictionCol),
      $(featuresCol),
      $(k),
      iteration,
      cost)

    model.setSummary(Some(summary))
    instr.logNamedValue("clusterSizes", summary.clusterSizes)
    model
  }

  private def initialize(dataset: Dataset[_]): Matrix = {
    val algo = new OldKMeans()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setInitializationSteps($(initSteps))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setEpsilon($(tol))
      .setDistanceMeasure($(distanceMeasure))

    val vectors = dataset.select(DatasetUtils.columnToVector(dataset, getFeaturesCol))
      .rdd
      .map { case Row(features: Vector) => OldVectors.fromML(features) }

    val instances = algo.initialize(vectors).map(_.asML)
    Matrices.fromVectors(instances)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

@Since("1.6.0")
object KMeans extends DefaultParamsReadable[KMeans] {

  @Since("1.6.0")
  override def load(path: String): KMeans = super.load(path)

  /** String name for random mode type. */
  private[clustering] val RANDOM = "random"

  /** String name for k-means|| mode type. */
  private[clustering] val K_MEANS_PARALLEL = "k-means||"

  private[clustering] val supportedInitModes = Array(RANDOM, K_MEANS_PARALLEL)

  /** String name for euclidean distance. */
  private[clustering] val EUCLIDEAN = "euclidean"

  /** String name for cosine distance. */
  private[clustering] val COSINE = "cosine"
}

/**
 * Summary of KMeans.
 *
 * @param predictions  `DataFrame` produced by `KMeansModel.transform()`.
 * @param predictionCol  Name for column of predicted clusters in `predictions`.
 * @param featuresCol  Name for column of features in `predictions`.
 * @param k  Number of clusters.
 * @param numIter  Number of iterations.
 * @param trainingCost K-means cost (sum of squared distances to the nearest centroid for all
 *                     points in the training dataset). This is equivalent to sklearn's inertia.
 */
@Since("2.0.0")
class KMeansSummary private[clustering] (
    predictions: DataFrame,
    predictionCol: String,
    featuresCol: String,
    k: Int,
    numIter: Int,
    @Since("2.4.0") val trainingCost: Double)
  extends ClusteringSummary(predictions, predictionCol, featuresCol, k, numIter)


private class KMeansAggregator (
    val bcCenters: Broadcast[Matrix],
    val k: Int,
    val numFeatures: Int,
    val blockSize: Int,
    val distanceMeasure: String) extends Serializable {
  import KMeans.{EUCLIDEAN, COSINE}

  var costSum = 0.0
  var count = 0L

  private lazy val sumMat =
    new DenseMatrix(k, numFeatures, Array.ofDim[Double](k * numFeatures))

  private lazy val weightSumVec = new DenseVector(Array.ofDim[Double](k))

  def weightSum: Double = weightSumVec.values.sum

  @transient private lazy val centerMat =
    bcCenters.value.toDense

  @transient private lazy val centerSquaredNorms = {
    distanceMeasure match {
      case EUCLIDEAN =>
        val k = centerMat.numRows
        val getNonZeroIter = this.getNonZeroIter(centerMat)
        Array.tabulate(k)(i => getNonZeroIter(i).map(t => t._2 * t._2).sum)
      case COSINE => null
    }
  }

  @transient private lazy val auxiliaryMat =
    new DenseMatrix(blockSize, k, Array.ofDim[Double](blockSize * k))

  def add(block: (Array[Double], Array[Double], Matrix)): this.type = {
    val (squaredNorms, weights, matrix) = block
    require(numFeatures == matrix.numCols, s"Dimensions mismatch when adding new " +
      s"instance. Expecting $numFeatures but got ${matrix.numCols}.")
    require(weights.forall(_ >= 0),
      s"instance weights ${weights.mkString("[", ",", "]")} has to be >= 0.0")

    val size = matrix.numRows
    count += size
    if (block._2.forall(_ == 0)) return this

    distanceMeasure match {
      case EUCLIDEAN =>
        euclideanUpdateInPlace(squaredNorms, weights, matrix)
      case COSINE =>
        cosineUpdateInPlace(weights, matrix)
    }

    this
  }

  private def euclideanUpdateInPlace(
      squaredNorms: Array[Double],
      weights: Array[Double],
      matrix: Matrix): Unit = {
    val size = matrix.numRows
    val localCenterSquaredNorms = centerSquaredNorms

    // mat here represents squared norms or cosine distance
    val mat = if (size == blockSize) {
      auxiliaryMat
    } else {
      new DenseMatrix(size, k, Array.ofDim[Double](size * k))
    }

    var i = 0
    var j = 0
    while (i < size) {
      j = 0
      val instanceSquaredNorm = squaredNorms(i)
      while (j < k) {
        val centerSquaredNorm = localCenterSquaredNorms(j)
        mat.values(i + j * size) = instanceSquaredNorm + centerSquaredNorm
        j += 1
      }
      i += 1
    }

    BLAS.gemm(-2.0, matrix, centerMat.transpose, 1.0, mat)

    // in-place convert squared euclidean distances to weights dispatched to each center
    // find the closest center and update costs and sums
    val getNonZeroIter = this.getNonZeroIter(matrix)
    val localWeightSumVec = weightSumVec
    val localSumMat = sumMat
    i = 0
    j = 0
    while (i < size) {
      val weight = weights(i)
      if (weight > 0) {
        var bestIndex = 0
        var bestSquaredDistance = Double.PositiveInfinity
        j = 0
        while (j < k) {
          val squaredDistance = mat(i, j)
          if (squaredDistance < bestSquaredDistance) {
            bestIndex = j
            bestSquaredDistance = squaredDistance
          }
          j += 1
        }

        costSum += weight * bestSquaredDistance
        localWeightSumVec.values(bestIndex) += weight
        if (weight == 1) {
          getNonZeroIter(i).foreach { case (j, v) =>
            localSumMat.values(bestIndex + j * k) += v
          }
        } else {
          getNonZeroIter(i).foreach { case (j, v) =>
            localSumMat.values(bestIndex + j * k) += v * weight
          }
        }
      }

      i += 1
    }
  }


  private def cosineUpdateInPlace(
      weights: Array[Double],
      matrix: Matrix): Unit = {
    val size = matrix.numRows

    // mat here represents cosine (NOT COSINE-DISTANCE!)
    val mat = if (size == blockSize) {
      auxiliaryMat
    } else {
      new DenseMatrix(size, k, Array.ofDim[Double](size * k))
    }

    BLAS.gemm(1.0, matrix, centerMat.transpose, 0.0, mat)

    // in-place convert cosine to weights dispatched to each center
    // find the closest center and update costs and sums
    val getNonZeroIter = this.getNonZeroIter(matrix)
    val localWeightSumVec = weightSumVec
    val localSumMat = sumMat
    var i = 0
    var j = 0
    while (i < size) {
      val weight = weights(i)
      if (weight > 0) {
        var bestIndex = 0
        var bestDistance = Double.PositiveInfinity
        j = 0
        while (j < k) {
          val distance = 1 - mat(i, j)
          if (distance < bestDistance) {
            bestIndex = j
            bestDistance = distance
          }
          j += 1
        }

        costSum += weight * bestDistance
        localWeightSumVec.values(bestIndex) += weight
        if (weight == 1) {
          getNonZeroIter(i).foreach { case (j, v) =>
            localSumMat.values(bestIndex + j * k) += v
          }
        } else {
          getNonZeroIter(i).foreach { case (j, v) =>
            localSumMat.values(bestIndex + j * k) += v * weight          }
        }
      }

      i += 1
    }
  }


  // directly get the non-zero iterator of i-th row vector without array copy or slice
  private def getNonZeroIter(matrix: Matrix): Int => Iterator[(Int, Double)] = {
    require(matrix.isTransposed)
    matrix match {
      case dm: DenseMatrix =>
        val numFeatures = dm.numCols
        (i: Int) =>
          val start = numFeatures * i
          Iterator.tabulate(numFeatures)(j =>
            (j, dm.values(start + j))
          ).filter(_._2 != 0)
      case sm: SparseMatrix =>
        (i: Int) =>
          val start = sm.colPtrs(i)
          val end = sm.colPtrs(i + 1)
          Iterator.tabulate(end - start)(j =>
            (sm.rowIndices(start + j), sm.values(start + j))
          ).filter(_._2 != 0)
    }
  }

  def merge(other: KMeansAggregator): this.type = {
    if (other.count != 0) {
      count += other.count
      costSum += other.costSum
      BLAS.axpy(1.0, other.weightSumVec, weightSumVec)
      BLAS.axpy(1.0, other.sumMat, sumMat)
    }

    this
  }

  def result: (Matrix, Array[Double]) = {
    val distanceFunc = distanceMeasure match {
      case EUCLIDEAN =>
        (v1: Vector, v2: Vector) => math.sqrt(Vectors.sqdist(v1, v2))
      case COSINE =>
        (v1: Vector, v2: Vector) =>
          val norm1 = Vectors.norm(v1, 2)
          val norm2 = Vectors.norm(v2, 2)
          require(norm1 > 0 && norm2 > 0,
            "Cosine distance is not defined for zero-length vectors.")
          1 - BLAS.dot(v1, v2) / norm1 / norm2
    }

    val prevCenters = bcCenters.value
    val newCenters = sumMat.rowIter
      .zip(weightSumVec.values.iterator)
      .zip(prevCenters.rowIter)
      .map { case ((sumVec, weightSum), prevCenter) =>
        val newCenter = if (weightSum > 0) {
          BLAS.scal(1.0 / weightSum, sumVec)
          sumVec
        } else {
          prevCenter.copy
        }

        distanceMeasure match {
          case COSINE =>
            val norm = Vectors.norm(newCenter, 2)
            require(norm > 0, "Cosine distance is not defined for zero-length vectors.")
            BLAS.scal(1.0 / norm, newCenter)
          case EUCLIDEAN =>
        }

        val distance = distanceFunc(newCenter, prevCenter)
        (newCenter, distance)
      }.toArray

    (Matrices.fromVectors(newCenters.map(_._1)), newCenters.map(_._2))
  }
}
