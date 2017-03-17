package de.model.logistic.regression


import java.io.{File, PrintWriter}

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.FullResult
import de.evaluation.util.SparkLOAN
import de.model.util.NumbersUtil
import de.model.util.NumbersUtil.round
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Model, Transformer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.Map
import scala.collection.immutable.Seq

/**
  * Finding the linear model of tools combinations;
  */

trait LogisticRegressionCommonBase {
  val experimentsConfig = ConfigFactory.load("experiments.conf")
  val trainFraction: Double = experimentsConfig.getDouble("train.fraction")
  val testFraction: Double = experimentsConfig.getDouble("test.fraction")

  val blackOakData = "blackoak"
  val hospData = "hosp"
  val salariesData = "salaries"

  val sep = ","
  val newLine = "\n"

  val tools = FullResult.tools.mkString(sep)
  val linearCombiHeader = s"dataset,intercept,$tools,threshold,trainP,trainR,trainF1"
  val linearCombiModelPath = experimentsConfig.getString("logistic.regression.result.csv")

  def write_to_file(path: String)(writer: PrintWriter => Unit) = {
    val file = new PrintWriter(new File(path))
    writer(file)
    file.close()
  }
}

class LogisticRegressionRunner extends LogisticRegressionCommonBase {
  private var libsvmFile = ""
  private var testLibsvmFile = ""
  private var trainLibsvmFile = ""
  private var eNetParam: Double = 0.0

  def onLibsvm(file: String): this.type = {
    libsvmFile = ConfigFactory.load().getString(file)
    this
  }

  def onTrainLibsvm(file: String): this.type = {
    trainLibsvmFile = ConfigFactory.load("experiments.conf").getString(file)
    this
  }

  def onTestLibsvm(file: String): this.type = {
    testLibsvmFile = ConfigFactory.load("experiments.conf").getString(file)
    this
  }

  def setElasticNetParam(param: Double): this.type = {
    eNetParam = param
    this
  }

  def findBestModel(): Unit = {
    SparkLOAN.withSparkSession("LOGREGRESSION") {
      session => {
        val training: DataFrame = session.read.format("libsvm").load(trainLibsvmFile)
        val test: DataFrame = session.read.format("libsvm").load(testLibsvmFile)
        //val Array(training, test) = data.randomSplit(Array(trainFraction, testFraction))

        val logRegr = new LogisticRegression()
        val paramGrid = new ParamGridBuilder()
          .addGrid(logRegr.maxIter, Array(100, 200, 250))
          .addGrid(logRegr.regParam, Array(0.1, 0.2, 0.3, 0.4, 0.5))
          .addGrid(logRegr.elasticNetParam, Array(0.0, 0.01, 0.4, 0.5, 0.7, 0.8, 0.9, 0.999))
          .build()

        val crossValidator = new CrossValidator()
          .setEstimator(logRegr)
          .setEvaluator(new BinaryClassificationEvaluator())
          .setEstimatorParamMaps(paramGrid)
          .setNumFolds(5)

        val crossValidatorModel: CrossValidatorModel = crossValidator.fit(training)

        val bestModel: Model[_] = crossValidatorModel.bestModel

        val bestModelParams: ParamMap = bestModel.extractParamMap()
        println(bestModelParams.toString())

        val evaluatedModel = evaluateRegressionModel(bestModel, test, FullResult.label)
        println(evaluatedModel.toString)

      }
    }
  }

  def runPredictions(ind: Int = 0, precisonBoundary: Double, recallBoundary: Double): (TrainData, TestData) = {

    var resultModel: Tuple2[TrainData, TestData]
    = (TrainData(0.0, 0.0, 0.5, Array(0.0), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      TestData(0L, 0L, 0.0, 0.0, 0.0, 0.0))


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
        val actualThreshold = 0.5
        /* logistic regression */
        val logRegression = new LogisticRegression()
          //.setTol(1E-8)
          //.setRegParam(regParam)
          .setElasticNetParam(elasticNetParam)
          .setThreshold(actualThreshold)

        val model = logRegression.fit(training)

        import NumbersUtil._

        val modelCoeff = model.coefficients.toArray.map(c => round(c))
        val intercept = round(model.intercept)

        //        println(s"Coefficients: ${modelCoeff.mkString(",")} -- Intercept: ${intercept}")

        val logisticRegressionTrainingSummary = model.summary

        val binarySummary = logisticRegressionTrainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

        val areaUnderROC = binarySummary.areaUnderROC

        import org.apache.spark.sql.functions.max

        val fMeasure = binarySummary.fMeasureByThreshold
        val preci = binarySummary.precisionByThreshold
        val recallByThreshold = binarySummary.recallByThreshold

        val maxF1 = fMeasure
          .join(preci, "threshold")
          .join(recallByThreshold, "threshold")
          .where(preci("precision") <= precisonBoundary && recallByThreshold("recall") <= recallBoundary)


        val Array(bestThreshold, bestF1, bestPrecision, bestRecall) = maxF1.count() == 0 match {
          case true => Array(0.5, 0.0, 0.0, 0.0)
          case false => {
            val selectMaxF1 = maxF1.select(max("F-Measure"))
            val maxFMeasure = selectMaxF1.head().getDouble(0)

            val bestAll: Row = maxF1
              .where(maxF1.col("F-Measure") === maxFMeasure)
              .select("threshold", "F-Measure", "precision", "recall")
              .head()

            val recall = bestAll.getDouble(3)
            val precision = bestAll.getDouble(2)
            val f1 = bestAll.getDouble(1)
            val threshold = bestAll.getDouble(0)
            Array(round(threshold, 4), round(f1, 4), round(precision, 4), round(recall, 4))
          }
        }


        //println(s"max F-Measure: $maxFMeasure")
        //println(s"best Threshold: $bestThreshold ")

        model.setThreshold(bestThreshold)

        val trainDataInfo = TrainData(
          regParam,
          elasticNetParam,
          bestThreshold,
          modelCoeff,
          intercept,
          round(bestF1),
          round(areaUnderROC),
          bestPrecision,
          bestRecall,
          bestF1)

        val testModel = ModelEvaluator.evaluateRegressionModel(model, test, FullResult.label)

        /*evaluateRegressionModel(model, test, FullResult.label)*/

        println(s"""$$ ${trainDataInfo.createModelFormula(ind)}$$   & ${bestPrecision} & ${bestRecall}   & ${bestF1}   & ${testModel.precision}   & ${testModel.recall}   & ${testModel.f1}  \\\\""")
        resultModel = (trainDataInfo, testModel)

      }
    }
    resultModel
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

    TestData(totalData, wrongPredictions.toLong, round(accuracy, 4), round(precision, 4), round(recall, 4), round(F1, 4))
  }


}

object BlackOakLogisticRegression extends LogisticRegressionCommonBase {
  def main(args: Array[String]): Unit = {
    run()
  }


  def run(): Tuple2[String, TrainData] = {
    val dataset = blackOakData
    println(dataset)

    //todo: p and r maximieren from baseline results
    //todo: write to file baseline
    val maxPrecision = 0.9773
    val maxRecall = 0.4739
    val logisticRegressionRunner = new LogisticRegressionRunner()
    logisticRegressionRunner.onLibsvm("model.full.result.file")

    //    logisticRegressionRunner.onTestLibsvm("blackoak.experiments.test.libsvm.file")
    //    logisticRegressionRunner.onTrainLibsvm("blackoak.experiments.train.libsvm.file")
    //    logisticRegressionRunner.findBestModel()

    logisticRegressionRunner.setElasticNetParam(0.1)
    val linearCombination =
      (1 to 15)
        .map(ind => logisticRegressionRunner.runPredictions(ind, maxPrecision, maxRecall))
    val allTrainData = linearCombination.map(_._1)
    val maxF1: Double = allTrainData.foldLeft(0.0)((max, c) => Math.max(max, c.maxFMeasure))
    val bestLinearCombi = linearCombination.filter(all => all._1.maxFMeasure == maxF1).head
    println(s"BEST LINEAR COMBI FOR $dataset")
    println(bestLinearCombi)
    (dataset, bestLinearCombi._1)


  }
}


object HospLogisticRegression extends LogisticRegressionCommonBase {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Tuple2[String, TrainData] = {
    val dataset = hospData
    println(dataset)
    //todo: p and r maximieren from baseline results
    //todo: write to file baseline
    val maxPrecision = 0.9279
    val maxRecall = 0.9994

    val logisticRegressionRunner = new LogisticRegressionRunner()
    logisticRegressionRunner.onLibsvm("model.hosp.10k.libsvm.file")

    //    logisticRegressionRunner.findBestModel()

    logisticRegressionRunner.setElasticNetParam(0.01)
    val linearCombination: Seq[(TrainData, TestData)] =
      (1 to 15)
        .map(ind => logisticRegressionRunner.runPredictions(ind, maxPrecision, maxRecall))
    val allTrainData = linearCombination.map(_._1)
    val maxF1: Double = allTrainData.foldLeft(0.0)((max, c) => Math.max(max, c.maxFMeasure))
    val bestLinearCombi = linearCombination.filter(all => all._1.maxFMeasure == maxF1).head
    println(s"BEST LINEAR COMBI FOR $dataset")
    println(bestLinearCombi)
    (dataset, bestLinearCombi._1)
  }
}

object SalariesLogisticRegression extends LogisticRegressionCommonBase {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Tuple2[String, TrainData] = {
    val dataset = "salaries"
    println(dataset)

    //todo: p and r maximieren from baseline results
    //todo: write to file baseline
    val maxPrecision = 0.7674
    val maxRecall = 0.1399
    val logisticRegressionRunner = new LogisticRegressionRunner()
    logisticRegressionRunner.onLibsvm("model.salaries.libsvm.file")
    //    logisticRegressionRunner.findBestModel()

    logisticRegressionRunner.setElasticNetParam(0.2)
    val linearCombination: Seq[(TrainData, TestData)] =
      (1 to 5)
        .map(ind => logisticRegressionRunner.runPredictions(ind, maxPrecision, maxRecall))
    val allTrainData = linearCombination.map(_._1)
    val maxF1: Double = allTrainData.foldLeft(0.0)((max, c) => Math.max(max, c.maxFMeasure))
    val bestLinearCombi = linearCombination.filter(all => all._1.maxFMeasure == maxF1).head
    println(s"BEST LINEAR COMBI FOR $dataset")
    println(bestLinearCombi)
    (dataset, bestLinearCombi._1)
  }
}

object LogisticRegressionRunAll extends LogisticRegressionCommonBase {
  def main(args: Array[String]): Unit = {

    val blackoakRes: (String, TrainData) = BlackOakLogisticRegression.run()
    val blackOakLine = constructLine(blackoakRes)

    val hospRes: (String, TrainData) = HospLogisticRegression.run()
    val hospLine = constructLine(hospRes)

    val salariesRes: (String, TrainData) = SalariesLogisticRegression.run()
    val salariesLine = constructLine(salariesRes)

    val allLines = Seq(blackOakLine, hospLine, salariesLine)

    //    write_to_file(linearCombiModelPath) {
    //      writer => {
    //        writer.write(s"$linearCombiHeader$newLine")
    //        allLines.foreach(line => {
    //          writer.write(s"$line$newLine")
    //        })
    //      }
    //    }
  }

  private def constructLine(res: (String, TrainData)) = {
    //    val linearCombiHeader = s"dataset,intercept,$tools,threshold,trainP,trainR,trainF1"
    val train: TrainData = res._2
    s"""${res._1},${train.intercept},${train.coefficients.mkString(sep)},${train.bestThreshold},${train.precision},${train.recall},${train.f1}"""
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

