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
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.collection.Map

/**
  * Finding the linear model of tools combinations;
  */

trait LogisticRegressionCommonBase {
  val experimentsConfig = ConfigFactory.load("experiments.conf")
  val trainFraction: Double = experimentsConfig.getDouble("train.fraction")
  val testFraction: Double = experimentsConfig.getDouble("test.fraction")
}

class LogisticRegressionRunner extends LogisticRegressionCommonBase {
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
        val Array(training, test) = data.randomSplit(Array(trainFraction, testFraction))

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
        val Array(training, test) = data.randomSplit(Array(trainFraction, testFraction))

        val elasticNetParam = ind % 3 match {
          case 1 => eNetParam //0.01
          case 2 => 0.999
          case 0 => 0.0
        }

        val regParam = 0.1
        /* logistic regression */
        val logRegression = new LogisticRegression()
          //.setTol(1E-8)
          //.setRegParam(regParam)
          .setElasticNetParam(elasticNetParam)
          .setThreshold(0.8)

        val model = logRegression.fit(training)

        import NumbersUtil._
        // Print the coefficients and intercept for logistic regression
        val modelCoeff = model.coefficients.toArray.map(c => round(c))
        val intercept = round(model.intercept)

        //        println(s"Coefficients: ${modelCoeff.mkString(",")} -- Intercept: ${intercept}")

        val logisticRegressionTrainingSummary = model.summary

        val binarySummary = logisticRegressionTrainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

        val lrRoc = binarySummary.roc
        //lrRoc.show()
        val areaUnderROC = binarySummary.areaUnderROC

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

        val f1 = ((p: Column, r: Column) => (p.*(r).*(2) / (p + r)))
        val pr: DataFrame = binarySummary.pr
        val withF1 = pr.withColumn("F1", f1(pr.col("precision"), pr.col("recall")))
        val topF1 = withF1.where(withF1.col("F1") === maxFMeasure)

        val trainPRF1: Row = topF1.head()
        val trainPrecision: Double = round(trainPRF1.getAs[Double]("precision"))
        val trainRecall: Double = round(trainPRF1.getAs[Double]("recall"))

        val trainDataInfo = TrainData(
          regParam,
          elasticNetParam,
          modelCoeff,
          intercept,
          round(maxFMeasure),
          round(areaUnderROC))

        val testDataInfo = evaluateRegressionModel(model, test, FullResult.label)

        println(s"""$$ ${trainDataInfo.createModelFormula(ind)}$$    & ${round(trainDataInfo.areaUnderRoc)} & ${trainPrecision} & ${trainRecall}   & ${trainDataInfo.maxFMeasure}   & ${testDataInfo.precision}   & ${testDataInfo.recall}   & ${testDataInfo.f1}  \\\\""")

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
    //val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val totalData = fullPredictions.count()
    //    println(s"Test data count: ${totalData}")
    //val labels = fullPredictions.select(labelColName).rdd.map(_.getDouble(0))
    val predictionAndGroundTruth = fullPredictions.select("prediction", labelColName)
    val zippedPredictionsAndLabels: RDD[(Double, Double)] = //predictions.zip(labels)
      predictionAndGroundTruth.rdd.map(row => {
        (row.getDouble(0), row.getDouble(1))
      })
    //val RMSE = new RegressionMetrics(zippedPredictionsAndLabels).rootMeanSquaredError
    //    println(s"  Root mean squared error (RMSE): $RMSE")


    val outcomeCounts: Map[(Double, Double), Long] = zippedPredictionsAndLabels.countByValue()
    //    println(s"count by values ${outcomeCounts}")


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
    //      .map(_._2)
    //      .foldLeft(0.0) { (acc, elem) => acc + elem }
    //    println(s"Wrong predictions: $wrongPredictions")

    TestData(totalData, wrongPredictions.toLong, round(accuracy, 4), round(precision, 4), round(recall, 4), round(F1, 4))
  }


}

object BlackOakLogisticRegression {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    println("BLACKOAK")
    val logisticRegressionRunner = new LogisticRegressionRunner()
    logisticRegressionRunner.onLibsvm("model.full.result.file")
    //    logisticRegressionRunner.findBestModel()

    logisticRegressionRunner.setElasticNetParam(0.2)
    (1 to 5).foreach(ind => logisticRegressionRunner.runPredictions(ind))
  }
}


object HospLogisticRegression {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    println("HOSP")
    val logisticRegressionRunner = new LogisticRegressionRunner()
    logisticRegressionRunner.onLibsvm("model.hosp.10k.libsvm.file")
    //    logisticRegressionRunner.findBestModel()

    logisticRegressionRunner.setElasticNetParam(0.01)
    (1 to 15).foreach(ind => logisticRegressionRunner.runPredictions(ind))
  }
}

object SalariesLogisticRegression {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {
    println("SALARIES")
    val logisticRegressionRunner = new LogisticRegressionRunner()
    logisticRegressionRunner.onLibsvm("model.salaries.libsvm.file")
    //    logisticRegressionRunner.findBestModel()

    logisticRegressionRunner.setElasticNetParam(0.2)
    (1 to 15).foreach(ind => logisticRegressionRunner.runPredictions(ind))
  }
}

object LogisticRegressionRunAll {
  def main(args: Array[String]): Unit = {
    BlackOakLogisticRegression.run()
    HospLogisticRegression.run()
    SalariesLogisticRegression.run()
  }
}

/**
  * blackoak
  *
  * {
  * logreg_74b4a4e8356d-elasticNetParam: 0.0,
  * logreg_74b4a4e8356d-featuresCol: features,
  * logreg_74b4a4e8356d-fitIntercept: true,
  * logreg_74b4a4e8356d-labelCol: label,
  * logreg_74b4a4e8356d-maxIter: 100,
  * logreg_74b4a4e8356d-predictionCol: prediction,
  * logreg_74b4a4e8356d-probabilityCol: probability,
  * logreg_74b4a4e8356d-rawPredictionCol: rawPrediction,
  * logreg_74b4a4e8356d-regParam: 0.2,
  * logreg_74b4a4e8356d-standardization: true,
  * logreg_74b4a4e8356d-threshold: 0.5,
  * logreg_74b4a4e8356d-tol: 1.0E-6
  * }
  * true positives: 0.0
  * TEST: Accuracy: 0.6276, Precision: 0.0, Recall: 0.0, F1: 0.0, totalTest: 440330, wrongPrediction: 163979
  *
  * hosp:
  *
  * {
  * logreg_8110e1c07c19-elasticNetParam: 0.0,
  * logreg_8110e1c07c19-featuresCol: features,
  * logreg_8110e1c07c19-fitIntercept: true,
  * logreg_8110e1c07c19-labelCol: label,
  * logreg_8110e1c07c19-maxIter: 100,
  * logreg_8110e1c07c19-predictionCol: prediction,
  * logreg_8110e1c07c19-probabilityCol: probability,
  * logreg_8110e1c07c19-rawPredictionCol: rawPrediction,
  * logreg_8110e1c07c19-regParam: 0.1,
  * logreg_8110e1c07c19-standardization: true,
  * logreg_8110e1c07c19-threshold: 0.5,
  * logreg_8110e1c07c19-tol: 1.0E-6
  * }
  * true positives: 2740.0
  * TEST: Accuracy: 0.9199, Precision: 0.9514, Recall: 0.194, F1: 0.3223, totalTest: 143760, wrongPrediction: 11522
  *
  * salaries:
  *
  * {
  * logreg_d60d7a031db2-elasticNetParam: 0.0,
  * logreg_d60d7a031db2-featuresCol: features,
  * logreg_d60d7a031db2-fitIntercept: true,
  * logreg_d60d7a031db2-labelCol: label,
  * logreg_d60d7a031db2-maxIter: 100,
  * logreg_d60d7a031db2-predictionCol: prediction,
  * logreg_d60d7a031db2-probabilityCol: probability,
  * logreg_d60d7a031db2-rawPredictionCol: rawPrediction,
  * logreg_d60d7a031db2-regParam: 0.1,
  * logreg_d60d7a031db2-standardization: true,
  * logreg_d60d7a031db2-threshold: 0.5,
  * logreg_d60d7a031db2-tol: 1.0E-6
  * }
  * true positives: 0.0
  * TEST: Accuracy: 0.989, Precision: 0.0, Recall: 0.0, F1: 0.0, totalTest: 252330, wrongPrediction: 2788
  *
  *
  *
  **/

