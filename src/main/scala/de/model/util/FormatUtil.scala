package de.model.util

import de.evaluation.f1.FullResult
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

/**
  * Created by visenger on 20/03/17.
  */
object FormatUtil {

  def getPredictionAndLabel(dataDF: DataFrame, column: String): RDD[(Double, Double)] = {
    dataDF.select(FullResult.label, column).rdd.map(row => {
      val label = row.getDouble(0)
      val prediction = row.getDouble(1)
      (prediction, label)
    })
  }

  def getPredictionAndLabelOnIntegers(dataDF: DataFrame, column: String): DataFrame = {
    val convertedDF: DataFrame = dataDF.select(FullResult.label, column)
      .withColumn("label-tmp", dataDF(FullResult.label).cast(DoubleType))
      .withColumn(s"$column-tmp",
        when(dataDF(column) === -1, lit(0.0).cast(DoubleType)).otherwise(lit(1.0).cast(DoubleType)))
      .drop(dataDF(FullResult.label))
      .drop(dataDF(column))
      .withColumnRenamed("label-tmp", FullResult.label)
      .withColumnRenamed(s"$column-tmp", "prediction")

    convertedDF.select("prediction", FullResult.label).toDF("prediction", "label")

//        dataDF.select(FullResult.label, column).rdd.map(row => {
//          val label = row.getInt(0).toDouble
//          val prediction = if (row.getInt(1) == -1) 0.0 else 1.0 // we change the error encoding to correspond to the label values
//          (prediction, label)
//        })
  }

  def getStringPredictionAndLabel(dataDF: DataFrame, column: String): RDD[(Double, Double)] = {
    //dataDF.printSchema()
    dataDF.select(FullResult.label, column).rdd.map(row => {
      val label = row.getString(0).toDouble
      val prediction = row.getString(1).toDouble
      (prediction, label)
    })
  }


  def prepareDataWithRowIdToLIBSVM(session: SparkSession, dataDF: DataFrame, tools: Seq[String]): DataFrame = {
    import session.implicits._
    val labelAndToolsCols = Seq(FullResult.label) ++ tools
    //row-id is LONG because it was introduced by  monotonically_increasing_id() function
    val labelAndTools = dataDF.select("row-id", labelAndToolsCols: _*)

    val data: DataFrame = labelAndTools.map(row => {
      val rowid: Long = row.getLong(0)
      val label: Double = row.get(1).toString.toDouble
      val toolsVals: Array[Double] = (2 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features = Vectors.dense(toolsVals)
      (rowid, label, features)
    }).toDF("row-id", "label", "features")

    data
  }

  def convertVectors(session: SparkSession, dataDF: DataFrame, features: String): DataFrame = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, features)

    val data: DataFrame = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = row.getAs[org.apache.spark.mllib.linalg.Vector](1).toArray
      val features = Vectors.dense(toolsVals)
      (label, features)
    }).toDF("label", "features")

    data
  }

  def prepareDataToLIBSVM(session: SparkSession, dataDF: DataFrame, tools: Seq[String]): DataFrame = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, tools: _*)

    val data: DataFrame = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features = Vectors.dense(toolsVals)
      (label, features)
    }).toDF("label", "features")

    data
  }

  def prepareDoublesToLIBSVM(session: SparkSession, dataDF: DataFrame, tools: Seq[String]): DataFrame = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, tools: _*)

    val data: DataFrame = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getDouble(idx)).toArray
      val features = Vectors.dense(toolsVals)
      (label, features)
    }).toDF("label", "features")

    data
  }

  def prepareDoublesToLabeledPoints(session: SparkSession, dataDF: DataFrame, tools: Seq[String]): RDD[LabeledPoint] = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, tools: _*)

    val data: RDD[LabeledPoint] = labelAndTools.map(row => {
      val label: Double = row.getDouble(0)
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getDouble(idx)).toArray
      val features = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      LabeledPoint(label, features)
    }).rdd

    data
  }

  def prepareDFToLabeledPointRDD(session: SparkSession, labelAndTools: DataFrame): RDD[LabeledPoint] = {

    import session.implicits._
    val data: RDD[LabeledPoint] = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val initVector: linalg.Vector = row.getAs[org.apache.spark.ml.linalg.Vector](1)
      val features = org.apache.spark.mllib.linalg.Vectors.dense(initVector.toArray)
      LabeledPoint(label, features)
    }).rdd
    data
  }

  def prepareTestDFToLabeledPointRDD(session: SparkSession, idsLabelAndTools: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._

    val convert_to_dense_vec = udf {
      (initVec: org.apache.spark.ml.linalg.Vector) => org.apache.spark.mllib.linalg.Vectors.dense(initVec.toArray)
    }

    val convert_to_double = udf {
      value: String => value.toDouble
    }

    val curatedLabelsFeaturesIdsDF: DataFrame = idsLabelAndTools
      .withColumn(s"${Features.featuresCol}-tmp", convert_to_dense_vec(idsLabelAndTools(Features.featuresCol)))
      .drop(Features.featuresCol)
      .withColumnRenamed(s"${Features.featuresCol}-tmp", Features.featuresCol)
      .withColumn(s"${FullResult.label}-tmp", convert_to_double(idsLabelAndTools(FullResult.label)))
      .drop(FullResult.label)
      .withColumnRenamed(s"${FullResult.label}-tmp", FullResult.label)
      // .select(FullResult.label, Seq(Features.featuresCol, FullResult.recid, FullResult.attrnr, FullResult.value) ++ FullResult.tools: _*)
      .toDF()
    curatedLabelsFeaturesIdsDF

  }

  def prepareDataToLabeledPoints(session: SparkSession, dataDF: DataFrame, tools: Seq[String]): RDD[LabeledPoint] = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, tools: _*)

    val data: RDD[LabeledPoint] = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      LabeledPoint(label, features)
    }).rdd

    data
  }

  def prepareToolsDataToLabPointsDF(session: SparkSession, dataDF: DataFrame, tools: Seq[String]): DataFrame = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, tools: _*)

    val data: DataFrame = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      (label, features)
    }).toDF("label", "features")

    data
  }

  def prepareClustersToLabeledPoints(session: SparkSession, dataDF: DataFrame, tools: Seq[String]): RDD[LabeledPoint] = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, tools: _*)

    val data: RDD[LabeledPoint] = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getDouble(idx)).toArray
      val features = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      LabeledPoint(label, features)
    }).rdd

    data
  }


}
