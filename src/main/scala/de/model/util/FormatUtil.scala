package de.model.util

import de.evaluation.f1.FullResult
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

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
      val initVector = row.getAs[org.apache.spark.ml.linalg.Vector](1)
      val features = org.apache.spark.mllib.linalg.Vectors.dense(initVector.toArray)
      LabeledPoint(label, features)
    }).rdd
    data
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
