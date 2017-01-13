package de.model.linear.regression

import de.evaluation.util.SparkSessionCreator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{GeneralizedLinearRegression, LinearRegression}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD

/**
  * Created by visenger on 29/12/16.
  */
class BlackOakGeneralizedLinearRegression {

}

object GeneralizedLinearRegressionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionCreator.createSession("GLR")

    // Load data
    val dataset = spark.read.format("libsvm")
      .load("src/main/resources/matrix-libsvm.txt")

    //todo: find the family
    val glr = new GeneralizedLinearRegression()
      //.setFamily("gaussian")
      //.setLink("log")
      .setMaxIter(200)
    // .setRegParam(0.0015)

    val paramGrid = new ParamGridBuilder()
      .addGrid(glr.family, Array("gaussian", "poisson", "gamma"))
      .addGrid(glr.regParam, Array(0.1, 0.01, 0.001, 0.0001, 0.00001))
      .addGrid(glr.tol, Array(1.0E-5, 1.0E-6, 1.0E-7))
      .addGrid(glr.maxIter, Array(150, 200, 250))
      .build()

    val Array(training, test) = dataset.randomSplit(Array(0.7, 0.3), seed = 1234L)


    val crossValidator = new CrossValidator()
      .setEstimator(glr)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)


    val validatorModel = crossValidator.fit(training)


    //predictForTestData.show(23)

    val bestModel = validatorModel.bestModel

    val bestModelParams: ParamMap = bestModel.extractParamMap()
    println(bestModelParams.toString())

    val evaluatorParams: ParamMap = crossValidator.getEvaluator.extractParamMap()
    println(evaluatorParams.toString())

    val linearRegr = new LinearRegression()


    //    test:


    val predictForTestData = validatorModel.transform(test)

    val prediction = predictForTestData.select("prediction").rdd.map(_.getDouble(0))
    val label = predictForTestData.select("label").rdd.map(_.getDouble(0))
    val predictionAndLabel: RDD[(Double, Double)] = prediction.zip(label)

    val regressionMetrics = new RegressionMetrics(predictionAndLabel)

    val rmse = regressionMetrics.rootMeanSquaredError
    println(s" Root mean squared error: $rmse")

    val model = linearRegr.fit(training, bestModelParams)
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")


    val predictAndLabelList = predictionAndLabel.collect().toList

    val closeness = predictAndLabelList.partition(p => Math.abs(p._1 - p._2) < 1.0)
    val closePrediction = closeness._1
    val total = predictAndLabelList.size
    val correct = closePrediction.size
    val precision = correct.toDouble / total.toDouble

    println(s" Precision: $precision")

    /*val path = ConfigFactory.load().getString("model.prediction.folder")
    predictForTestData
      .rdd.
      coalesce(1)
      .saveAsTextFile(path)*/


    /* // Fit the model
     val model = glr.fit(training)

     // Print the coefficients and intercept for generalized linear regression model
     println(s"Coefficients: ${model.coefficients}")
     println(s"Intercept: ${model.intercept}")*/

    // Summarize the model over the training set and print out some metrics
    //    val summary = model.summary
    //    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    //    println(s"T Values: ${summary.tValues.mkString(",")}")
    //    println(s"P Values: ${summary.pValues.mkString(",")}")
    //    println(s"Dispersion: ${summary.dispersion}")
    //    println(s"Null Deviance: ${summary.nullDeviance}")
    //    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    //    println(s"Deviance: ${summary.deviance}")
    //    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    //    println(s"AIC: ${summary.aic}")
    //    println("Deviance Residuals: ")
    //summary.residuals().show()


    /* val prediction = model.transform(test)
     prediction.show(100)

     val predictionVals = prediction.select("prediction").rdd.map(_.getDouble(0))
     val labelValues = prediction.select("label").rdd.map(_.getDouble(0))

     val regressionMetrics = new RegressionMetrics(predictionVals.zip(labelValues))
     val rmse = regressionMetrics.rootMeanSquaredError
     println(s" Root Mean Squared Error: $rmse")*/


    spark.stop()
  }
}


/**
  * Best model for the BlackOak data:
  * {
  * glm_8a3d083546aa-family: gaussian,
  * glm_8a3d083546aa-featuresCol: features,
  * glm_8a3d083546aa-fitIntercept: true,
  * glm_8a3d083546aa-labelCol: label,
  * glm_8a3d083546aa-maxIter: 150,
  * glm_8a3d083546aa-predictionCol: prediction,
  * glm_8a3d083546aa-regParam: 0.001,
  * glm_8a3d083546aa-solver: irls,
  * glm_8a3d083546aa-tol: 1.0E-5
  * }
  * {
  * regEval_417b6499109a-labelCol: label,
  * regEval_417b6499109a-metricName: rmse,
  * regEval_417b6499109a-predictionCol: prediction
  * }
  * Root mean squared error: 1.988505840926187
  * Coefficients: [3.763641225180124E-5,-1.9097264485822973E-5,-6.341549625392139E-6,1.389271250648209E-7,-3.921393815321451E-6]
  * Intercept: 7.113492185440937
  * Precision: 0.5045995022582727
  *
  *
  **/