package de.model.logistic.regression

import de.model.util.NumbersUtil.round
import org.apache.spark.ml.Transformer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.Map

/**
  * Computes Precision, Recall and F1 of the predictions.
  */
object ModelEvaluator {

  /**
    * Evaluate the given RegressionModel on test data.
    *
    * @param model        Must fit RegressionModel abstraction
    * @param data         DataFrame with "features" and labelColName columns
    * @param labelColName Name of the labelCol parameter for the model
    *
    */
  def evaluateRegressionModel(model: Transformer,
                              data: DataFrame,
                              labelColName: String): TestData = {
    val fullPredictions = model.transform(data).cache()
    //fullPredictions.show(200)
    val totalData = fullPredictions.count()
    val predictionAndGroundTruth = fullPredictions.select("prediction", labelColName)
    val zippedPredictionsAndLabels: RDD[(Double, Double)] =
      predictionAndGroundTruth.rdd.map(row => {
        (row.getDouble(0), row.getDouble(1))
      })
    //    val RMSE = new RegressionMetrics(zippedPredictionsAndLabels).rootMeanSquaredError
    //    println(s"  Root mean squared error (RMSE): $RMSE")


    val outcomeCounts: Map[(Double, Double), Long] = zippedPredictionsAndLabels.countByValue()

    var tp = 0.0
    var fn = 0.0
    var tn = 0.0
    var fp = 0.0

    outcomeCounts.foreach(elem => {
      val values = elem._1
      val count = elem._2
      values match {
        //(prediction, label)
        case (0.0, 0.0) => tn = count
        case (1.0, 1.0) => tp = count
        case (1.0, 0.0) => fp = count
        case (0.0, 1.0) => fn = count
      }
    })

    //    println(s"true positives: $tp")

    val accuracy = (tp + tn) / totalData.toDouble
    //    println(s"Accuracy: $accuracy")
    val precision = tp / (tp + fp).toDouble
    //    println(s"Precision: $precision")

    val recall = tp / (tp + fn).toDouble
    //    println(s"Recall: $recall")

    val F1 = 2 * precision * recall / (precision + recall)
    //    println(s"F-1 Score: $F1")

    val wrongPredictions: Double = outcomeCounts
      .count(key => key._1 != key._2)

    val testData = TestData(totalData, wrongPredictions.toLong, round(accuracy, 4), round(precision, 4), round(recall, 4), round(F1, 4))
    //println(testData)
    testData
  }


}
