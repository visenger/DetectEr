package de.model.util

import de.evaluation.f1.F1
import de.model.logistic.regression.ModelData
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by visenger on 29/03/17.
  */
object ModelUtil {

  def getBestModel(maxPrecision: Double,
                   maxRecall: Double,
                   train: RDD[LabeledPoint],
                   test: RDD[LabeledPoint]): (ModelData, LogisticRegressionModel) = {

    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .setIntercept(true)
      .run(train)

    val allThresholds = Seq(0.6, 0.55, 0.53, 0.5, 0.45, 0.4,
      0.39, 0.38, 0.377, 0.375, 0.374, 0.37,
      0.369, 0.368, 0.3675, 0.367, 0.365, 0.36, 0.34, 0.33, 0.32, 0.31,
      0.3, 0.25, 0.2, 0.17, 0.15, 0.13, 0.1, 0.09, 0.05, 0.01)

    val allModels: Seq[ModelData] = allThresholds.map(τ => {
      model.setThreshold(τ)
      val predictionAndLabels: RDD[(Double, Double)] = test.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
      val testResult = F1.evalPredictionAndLabels(predictionAndLabels)

      val coefficients: Array[Double] = model.weights.toArray
      val intercept: Double = model.intercept

      ModelData(model.getThreshold.get,
        coefficients,
        intercept,
        testResult.precision,
        testResult.recall,
        testResult.f1)
    })

    val acceptableTests = allModels.filter(modelData =>
      modelData.precision <= maxPrecision
        && modelData.recall <= maxRecall)

    val modelData: ModelData = if (acceptableTests.nonEmpty) {
      val sortedList = acceptableTests
        .sortWith((t1, t2) => t1.f1 >= t2.f1)
      val bestModel: ModelData = sortedList.head
      bestModel
    } else {
      //empty model data
      ModelData.emptyModel
    }
    (modelData, model)
  }

  def getBestModel(train: RDD[LabeledPoint],
                   test: RDD[LabeledPoint]): (ModelData, LogisticRegressionModel) = {

    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .setIntercept(true)
      .run(train)

    //todo: REMOVE THRESHOLDS
    val allThresholds = Seq(0.6, 0.55, 0.53, 0.5, 0.45, 0.4,
      0.39, 0.38, 0.377, 0.375, 0.374, 0.37,
      0.369, 0.368, 0.3675, 0.367, 0.365, 0.36, 0.34, 0.33, 0.32, 0.31,
      0.3, 0.25, 0.2, 0.17, 0.15, 0.13, 0.1, 0.09, 0.05, 0.01)

    val allModels: Seq[ModelData] = allThresholds.map(τ => {
      model.setThreshold(τ)
      val predictionAndLabels: RDD[(Double, Double)] = test.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
      val testResult = F1.evalPredictionAndLabels(predictionAndLabels)

      val coefficients: Array[Double] = model.weights.toArray
      val intercept: Double = model.intercept

      ModelData(model.getThreshold.get,
        coefficients,
        intercept,
        testResult.precision,
        testResult.recall,
        testResult.f1)
    })
    val maxPrecision = 1.0
    val maxRecall = 1.0
    val acceptableTests = allModels.filter(modelData =>
      modelData.precision <= maxPrecision
        && modelData.recall <= maxRecall)

    val modelData: ModelData = if (acceptableTests.nonEmpty) {
      val sortedList = acceptableTests
        .sortWith((t1, t2) => t1.f1 >= t2.f1)
      val bestModel: ModelData = sortedList.head
      bestModel
    } else {
      //empty model data
      ModelData.emptyModel
    }
    (modelData, model)
  }


}
