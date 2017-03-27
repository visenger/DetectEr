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
