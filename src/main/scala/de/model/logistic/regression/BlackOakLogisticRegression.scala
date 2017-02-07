package de.model.logistic.regression

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{FullResult, Table}
import de.evaluation.util.{DataSetCreator, SparkSessionCreator}
import org.apache.spark.ml.{Model, Transformer}
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, BinaryLogisticRegressionTrainingSummary, LogisticRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by visenger on 06/02/17.
  */
object BlackOakLogisticRegression {

  def main(args: Array[String]): Unit = {
    val path = ConfigFactory.load().getString("model.full.result.file")
    val sparkSession: SparkSession = SparkSessionCreator.createSession("LOGREGR")

    val data = sparkSession.read.format("libsvm").load(path)
    val Array(training, test) = data.randomSplit(Array(0.8, 0.2))

    /* val logRegr = new LogisticRegression()


     val paramGrid = new ParamGridBuilder()
       .addGrid(logRegr.maxIter, Array(100, 200, 250))
       .addGrid(logRegr.regParam, Array(0.1, 0.2, 0.3, 0.4, 0.5))
       .addGrid(logRegr.elasticNetParam, Array(0.01, 0.4, 0.5, 0.7, 0.8, 0.9, 0.999))
       .build()

     val crossValidator = new CrossValidator()
       .setEstimator(logRegr)
       .setEvaluator(new RegressionEvaluator())
       .setEstimatorParamMaps(paramGrid)
       .setNumFolds(5)

     val crossValidatorModel: CrossValidatorModel = crossValidator.fit(training)

     val bestModel = crossValidatorModel.bestModel

     val bestModelParams: ParamMap = bestModel.extractParamMap()
     println(bestModelParams.toString())

     evaluateRegressionModel(bestModel, test, FullResult.label)*/


    println(s"-----------the previous model--------------")

    /* logistic regression */
    val logRegression = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.1)
      .setElasticNetParam(0.4)

    val model = logRegression.fit(training)

    // Print the coefficients and intercept for logistic regression
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")


    val logisticRegressionTrainingSummary = model.summary

    //    val objectiveHistory = logisticRegressionTrainingSummary.objectiveHistory
    //
    //    println(s"Objective History: ${objectiveHistory.foreach(loss => println(loss))}")

    val binarySummary = logisticRegressionTrainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val lrRoc = binarySummary.roc
    lrRoc.show()
    println(s"Area under ROC: ${binarySummary.areaUnderROC}")

    import org.apache.spark.sql.functions._
    val fMeasure = binarySummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure
      .where(fMeasure.col("F-Measure") === maxFMeasure)
      .select("threshold")
      .head()
      .getDouble(0)

    println(s"max F-Measure: $maxFMeasure")

    model.setThreshold(bestThreshold)

    val prediction = model.transform(test)

    prediction.show(4)

    evaluateRegressionModel(model, test, FullResult.label)


    sparkSession.stop()
  }

  /**
    * Evaluate the given RegressionModel on data. Print the results.
    *
    * @param model        Must fit RegressionModel abstraction
    * @param data         DataFrame with "prediction" and labelColName columns
    * @param labelColName Name of the labelCol parameter for the model
    *
    */
  private def evaluateRegressionModel(model: Transformer,
                                      data: DataFrame,
                                      labelColName: String): Unit = {
    val fullPredictions = model.transform(data).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    println(s"Test data count: ${predictions.count()}")
    val labels = fullPredictions.select(labelColName).rdd.map(_.getDouble(0))
    val zippedPredictionsAndLabels: RDD[(Double, Double)] = predictions.zip(labels)
    val RMSE = new RegressionMetrics(zippedPredictionsAndLabels).rootMeanSquaredError
    println(s"  Root mean squared error (RMSE): $RMSE")
    val wrongPredictions = zippedPredictionsAndLabels.filter(t => {
      Math.abs(t._1 - t._2) != 0
    }).count()
    println(s"Wrong predictions count: $wrongPredictions")

    println(s"count by values ${zippedPredictionsAndLabels.countByValue()}")
  }

  private def getDataFrame(session: SparkSession): DataFrame = {
    val path = ConfigFactory.load().getString("output.full.result.file")
    val data = DataSetCreator.createDataSetNoHeader(session, path, FullResult.schema: _*)
    data
  }

}
