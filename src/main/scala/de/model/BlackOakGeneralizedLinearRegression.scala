package de.model

import de.evaluation.util.SparkSessionCreator
import org.apache.spark.ml.regression.GeneralizedLinearRegression

/**
  * Created by visenger on 29/12/16.
  */
class BlackOakGeneralizedLinearRegression {

}

object GeneralizedLinearRegressionExample {
  def main(args: Array[String]): Unit = {
    var spark = SparkSessionCreator.createSession("GLR")

    // Load data
    val dataset = spark.read.format("libsvm")
      .load("src/main/resources/matrix-libsvm.txt")

    //todo: find the family
    val glr = new GeneralizedLinearRegression()
      .setFamily("gamma")
      .setLink("inverse")
      .setMaxIter(100)
      .setRegParam(0.15)

    val Array(training, test) = dataset.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Fit the model
    val model = glr.fit(training)

    // Print the coefficients and intercept for generalized linear regression model
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    val summary = model.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    //summary.residuals().show()


    val prediction = model.transform(test)
    prediction.show(12)

    spark.stop()
  }
}


