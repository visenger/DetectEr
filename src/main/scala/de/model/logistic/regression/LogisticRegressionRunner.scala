package de.model.logistic.regression


import com.typesafe.config.ConfigFactory
import de.evaluation.f1.FullResult
import de.evaluation.util.SparkLOAN
import de.model.util.NumbersUtil
import de.model.util.NumbersUtil.round
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 16/02/17.
  */
class LogisticRegressionRunner {
  private var libsvmFile = ""
  private var eNetParam: Double = 0.0

  def onLibsvm(file: String): this.type = {
    libsvmFile = ConfigFactory.load().getString(file)
    this
  }

  def setElasticNetParam(param: Double): this.type = {
    eNetParam = param
    this
  }

  def findBestModel(): Unit = {
    SparkLOAN.withSparkSession("LOGREGRESSION") {
      session => {
        val data = session.read.format("libsvm").load(libsvmFile)
        val Array(training, test) = data.randomSplit(Array(0.8, 0.2))

        val logRegr = new LogisticRegression()
        val paramGrid = new ParamGridBuilder()
          .addGrid(logRegr.maxIter, Array(100, 200, 250))
          .addGrid(logRegr.regParam, Array(0.1, 0.2, 0.3, 0.4, 0.5))
          .addGrid(logRegr.elasticNetParam, Array(0.0, 0.01, 0.4, 0.5, 0.7, 0.8, 0.9, 0.999))
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

        val evaluatedModel = evaluateRegressionModel(bestModel, test, FullResult.label)
        println(evaluatedModel.toString)

      }
    }
  }

  def runPredictions(ind: Int = 0): Unit = {

    SparkLOAN.withSparkSession("LOGREGRESSION") {
      session => {
        val data = session.read.format("libsvm").load(libsvmFile)
        val Array(training, test) = data.randomSplit(Array(0.7, 0.3))


        val elasticNetParam = ind % 2 == 0 match {
          case true => eNetParam //0.01
          case false => 0.4
        }

        val regParam = 0.1
        /* logistic regression */
        val logRegression = new LogisticRegression()
          .setTol(1E-8)
          .setRegParam(regParam)
          .setElasticNetParam(elasticNetParam)


        val model = logRegression.fit(training)

        import NumbersUtil._
        // Print the coefficients and intercept for logistic regression
        val modelCoeff = model.coefficients.toArray.map(c => round(c, 4))
        val intercept = round(model.intercept, 4)

        //        println(s"Coefficients: ${modelCoeff.mkString(",")} -- Intercept: ${intercept}")


        val logisticRegressionTrainingSummary = model.summary

        //    val objectiveHistory = logisticRegressionTrainingSummary.objectiveHistory
        //
        //    println(s"Objective History: ${objectiveHistory.foreach(loss => println(loss))}")

        val binarySummary = logisticRegressionTrainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

        val lrRoc = binarySummary.roc
        //lrRoc.show()
        val areaUnderROC = binarySummary.areaUnderROC
        //        println(s"Area under ROC: ${areaUnderROC}")

        import org.apache.spark.sql.functions.max
        val fMeasure = binarySummary.fMeasureByThreshold

        val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
        val bestThreshold = fMeasure
          .where(fMeasure.col("F-Measure") === maxFMeasure)
          .select("threshold")
          .head()
          .getDouble(0)

        //        println(s"max F-Measure: $maxFMeasure")
        //        println(s"best Threshold: $bestThreshold")

        model.setThreshold(bestThreshold)

        val trainDataInfo = TrainData(regParam, elasticNetParam, modelCoeff, intercept, round(maxFMeasure, 4), round(areaUnderROC, 4))

        //val prediction = model.transform(test)

        //prediction.show(4)

        val testDataInfo = evaluateRegressionModel(model, test, FullResult.label)

        //        println(s" round $ind:")
        //        println(trainDataInfo)
        //        println(trainDataInfo.createModelFormula(ind))
        //        println(testDataInfo)
        //func         & auc             & train f1          & p         & r      & test f1                 \\

        println(s"""$$ ${trainDataInfo.createModelFormula(ind)}$$     & ${trainDataInfo.areaUnderRoc}    & ${trainDataInfo.maxFMeasure}   & ${testDataInfo.precision}   & ${testDataInfo.recall}   & ${testDataInfo.f1}  \\\\""")
        //println(s"---------------------------------------------------------")

      }
    }

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
                                      labelColName: String): TestData = {
    val fullPredictions = model.transform(data).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val totalData = predictions.count()
    //    println(s"Test data count: ${totalData}")
    val labels = fullPredictions.select(labelColName).rdd.map(_.getDouble(0))
    val zippedPredictionsAndLabels: RDD[(Double, Double)] = predictions.zip(labels)
    val RMSE = new RegressionMetrics(zippedPredictionsAndLabels).rootMeanSquaredError
    //    println(s"  Root mean squared error (RMSE): $RMSE")


    val outcomeCounts = zippedPredictionsAndLabels.countByValue()
    //    println(s"count by values ${outcomeCounts}")

    val wrongPredictions: Double = outcomeCounts
      .filterKeys(key => key._1 != key._2)
      .map(_._2)
      .foldLeft(0.0) { (acc, elem) => acc + elem }
    //    println(s"Wrong predictions: $wrongPredictions")

    var tp = 0.0
    var fn = 0.0
    var tn = 0.0
    var fp = 0.0

    outcomeCounts.foreach(elem => {
      val values = elem._1
      val count = elem._2
      values match {
        case (0.0, 0.0) => tn = count
        case (1.0, 1.0) => tp = count
        case (1.0, 0.0) => fp = count
        case (0.0, 1.0) => fn = count
      }
    })

    println(s"true positives: $tp")

    val accuracy = (tp + tn) / totalData.toDouble
    //    println(s"Accuracy: $accuracy")
    val precision = tp / (tp + fp).toDouble
    //    println(s"Precision: $precision")

    val recall = tp / (tp + fn).toDouble
    //    println(s"Recall: $recall")

    val F1 = 2 * precision * recall / (precision + recall)
    //    println(s"F-1 Score: $F1")

    TestData(totalData, wrongPredictions.toLong, round(accuracy, 4), round(precision, 4), round(recall, 4), round(F1, 4))
  }


}


object HospLogisticRegression {
  def main(args: Array[String]): Unit = {
    val logisticRegressionRunner = new LogisticRegressionRunner()

    logisticRegressionRunner.onLibsvm("model.salaries.libsvm.file")
    //logisticRegressionRunner.findBestModel()

    /** BEST MODEL:
      * //logisticRegressionRunner.findBestModel()
      * {
      * logreg_adc0a80c0ca7-elasticNetParam: 0.01,
      * logreg_adc0a80c0ca7-featuresCol: features,
      * logreg_adc0a80c0ca7-fitIntercept: true,
      * logreg_adc0a80c0ca7-labelCol: label,
      * logreg_adc0a80c0ca7-maxIter: 100,
      * logreg_adc0a80c0ca7-predictionCol: prediction,
      * logreg_adc0a80c0ca7-probabilityCol: probability,
      * logreg_adc0a80c0ca7-rawPredictionCol: rawPrediction,
      * logreg_adc0a80c0ca7-regParam: 0.1,
      * logreg_adc0a80c0ca7-standardization: true,
      * logreg_adc0a80c0ca7-threshold: 0.5,
      * logreg_adc0a80c0ca7-tol: 1.0E-6
      * }
      * TEST: Accuracy: 0.921, Precision: 0.9633, Recall: 0.2068, F1: 0.3405, totalTest: 32209, wrongPrediction: 2545
      * */

    logisticRegressionRunner.setElasticNetParam(0.0)
    (1 to 15).foreach(ind => logisticRegressionRunner.runPredictions(ind))

  }
}

