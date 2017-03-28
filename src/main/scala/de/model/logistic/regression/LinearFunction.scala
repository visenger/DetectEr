package de.model.logistic.regression

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{Eval, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.FormatUtil
import de.model.util.NumbersUtil.round
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.Map


/**
  * Created by visenger on 06/03/17.
  */
class LinearFunction {

  private val config = ConfigFactory.load("experiments.conf")

  private var trainDataPath = ""
  private var testDataPath = ""

  //default values:
  private var toolsForLinearCombi: Seq[String] = FullResult.tools
  private var maxPrecision = 0.0
  private var maxRecall = 0.0
  private var maximumF1 = 0.0
  private var minimumF1 = 0.0

  private var datasetName = ""

  def onDatasetName(data: String): this.type = {
    datasetName = data.toLowerCase
    val dataTrainFile = s"${datasetName}.experiments.train.file"
    trainDataPath = config.getString(dataTrainFile)

    val dataTestFile = s"${datasetName}.experiments.test.file"
    testDataPath = config.getString(dataTestFile)

    maxPrecision = config.getDouble(s"${datasetName}.max.precision")
    maxRecall = config.getDouble(s"${datasetName}.max.recall")

    maximumF1 = config.getDouble(s"${datasetName}.max.F1")
    minimumF1 = config.getDouble(s"${datasetName}.min.F1")

    this
  }

  def onTools(tools: Seq[String]): this.type = {
    toolsForLinearCombi = tools
    this
  }


  def learn(): Eval = {
    var eval: Eval = null
    SparkLOAN.withSparkSession("LEARNERONSELECTION") {
      session => {
        eval = evaluateLinearCombi(session)
      }
    }
    Eval(eval.precision, eval.recall, eval.f1)

  }

  def learnWithLBFGS(): Eval = {
    var eval: Eval = null
    SparkLOAN.withSparkSession("LBFGS") {
      session => {
        eval = evaluateLinearCombiWithLBFGS(session)
      }
    }
    Eval(eval.precision, eval.recall, eval.f1, eval.info)

  }

  def evaluateLinearCombiWithLBFGS(session: SparkSession): Eval = {
    val trainDataDF: DataFrame = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*)
    val testDataDF = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*)

    val train: RDD[LabeledPoint] = FormatUtil.prepareDataToLabeledPoints(session, trainDataDF, toolsForLinearCombi)
    val test = FormatUtil.prepareDataToLabeledPoints(session, testDataDF, toolsForLinearCombi)

    val eval = trainModelWithLBFGS(train, test)

    eval
  }

  @Deprecated
  def evaluateLinearCombi(session: SparkSession): Eval = {
    val dataDF: DataFrame = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*)
    val train: DataFrame = prepareDataToLIBSVM(session, dataDF)

    val maxIteration = 18
    //todo: magic numbers!
    val allModels: Seq[(LogisticRegressionModel, Double)] = (1 to maxIteration)
      .map(idx => trainModel(idx, train))

    val maxF1 = allModels
      .map(_._2)
      .foldLeft(0.0)((max, f1) => Math.max(max, f1))

    val bestModel: (LogisticRegressionModel, Double) = allModels.filter(modelF1 => modelF1._2 == maxF1).head

    val model: LogisticRegressionModel = bestModel._1


    /// PREDICTION:
    val testDataDF = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*)

    val testDataLIBSVM = prepareDataToLIBSVM(session, testDataDF)

    val testData: TestData = ModelEvaluator.evaluateRegressionModel(model, testDataLIBSVM, FullResult.label)


    //println(testData.toString)

    //todo: try another threshold
    //    model.setThreshold(0.19)
    //    val eval = ModelEvaluator.evaluateRegressionModel(model, testDataLIBSVM, FullResult.label)
    //    println(eval.toString)
    //

    val coefficients: Array[Double] = model.coefficients.toArray
    val intercept: Double = model.intercept
    val modelFormula = createModelFormula(intercept, coefficients)
    //println(modelFormula)

    Eval(testData.precision, testData.recall, testData.f1, modelFormula)

  }


  private def trainModel(ind: Int = 0, train: DataFrame): (LogisticRegressionModel, Double) = {

    val logRegr = new LogisticRegression()

    val elasticNetParam = ind % 9 match {
      case 0 => 0.0
      case 1 => 0.01 //0.01
      case 2 => 0.1
      case 3 => 0.2
      case 4 => 0.45
      case 5 => 0.6
      case 6 => 0.7
      case 7 => 0.8
      case 8 => 0.999
    }

    val regParam = 0.5
    val actualThreshold = 0.5
    /* logistic regression */
    logRegr
      //.setTol(1E-8)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setThreshold(actualThreshold)

    //    val paramMap = ParamMap()
    //    paramMap.put(logRegr.elasticNetParam -> 0.0)
    //          .put(lr.maxIter, 30) // Specify 1 Param. This overwrites the original maxIter.
    //          .put(lr.regParam -> 0.8, lr.threshold -> 0.34)
    // Specify multiple Params.

    val model = logRegr.fit(train)

    val logisticRegressionTrainingSummary = model.summary

    val binarySummary = logisticRegressionTrainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val areaUnderROC = binarySummary.areaUnderROC
    val fMeasure = binarySummary.fMeasureByThreshold
    val preci = binarySummary.precisionByThreshold
    val recall = binarySummary.recallByThreshold

    val maxF1 = fMeasure
      .join(preci, "threshold")
      .join(recall, "threshold")
      .where(
        preci("precision") <= maxPrecision
          && recall("recall") <= maxRecall
        //&& fMeasure("F-Measure") <= maximumF1
        // && fMeasure("F-Measure") > minimumF1
      )

    maxF1.show()

    import org.apache.spark.sql.functions.max
    val Array(bestThreshold, bestF1, bestPrecision, bestRecall) = maxF1.count() == 0 match {
      case true => Array(0.5, 0.0, 0.0, 0.0)
      case false => {
        val selectMaxF1 = maxF1.select(max("F-Measure"))
        val maxFMeasure = selectMaxF1.head().getDouble(0)

        val bestAll: Row = maxF1
          .where(maxF1.col("F-Measure") === maxFMeasure)
          .select("threshold", "F-Measure", "precision", "recall")
          .head()

        val threshold = bestAll.getDouble(0)
        val f1 = bestAll.getDouble(1)
        val precision = bestAll.getDouble(2)
        val recall = bestAll.getDouble(3)
        Array(round(threshold, 4), round(f1, 4), round(precision, 4), round(recall, 4))
      }
    }

    //println(s"max F-Measure: $maxFMeasure")
    //println(s"best Threshold: $bestThreshold ")
    //println(s"elasticNetParam: ${model.elasticNetParam}")

    model.setThreshold(bestThreshold)

    //println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    val trainDataInfo = TrainData(
      regParam,
      elasticNetParam,
      bestThreshold,
      model.coefficients.toArray,
      model.intercept,
      round(bestF1),
      round(areaUnderROC),
      bestPrecision,
      bestRecall,
      bestF1)

    println(trainDataInfo)
    (model, round(bestF1))
  }

  private def trainModelWithLBFGS(train: RDD[LabeledPoint],
                                  test: RDD[LabeledPoint]): Eval = {


    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .setIntercept(true)
      .run(train /*, initialWeights*/)

    // println(initialWeights.toJson)

    val allThresholds = Seq(0.5, 0.45, 0.4,
      0.39, 0.38, 0.377, 0.375, 0.374, 0.37,
      0.369, 0.368, 0.3675, 0.367, 0.365, 0.36, 0.3, 0.25, 0.2, 0.17, 0.15, 0.13, 0.1, 0.09, 0.05, 0.01)

    val alltestResults: Seq[TestData] = allThresholds.map(τ => {
      model.setThreshold(τ)
      val predictionAndLabels: RDD[(Double, Double)] = test.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
      val testResult = evalPredictionAndLabels(predictionAndLabels)

      val coefficients: Array[Double] = model.weights.toArray
      val intercept: Double = model.intercept
      val modelFormula = createModelFormula(intercept, coefficients)

      //      if (testResult.precision <= maxPrecision && testResult.recall <= maxRecall) {
      //        println(s"models threshold: ${model.getThreshold.get}")
      //        println(testResult)
      //      }

      TestData(testResult.totalTest,
        testResult.wrongPrediction,
        testResult.accuracy,
        testResult.precision,
        testResult.recall,
        testResult.f1,
        modelFormula)
    })

    val acceptableTests = alltestResults.filter(testData =>
      testData.precision <= maxPrecision
        && testData.recall <= maxRecall)

    val eval = if (acceptableTests.nonEmpty) {
      val sortedList = acceptableTests
        .sortWith((t1, t2) => t1.accuracy >= t2.accuracy)
        .sortWith((t1, t2) => t1.f1 >= t2.f1)

      val maxTestResult = sortedList.head
      Eval(maxTestResult.precision, maxTestResult.recall, maxTestResult.f1, maxTestResult.info)
    } else {
      Eval(0.0, 0.0, 0.0, "")
    }

    eval

  }

  private def initialWeights: org.apache.spark.mllib.linalg.Vector = {
    val weights: Seq[Double] = toolsForLinearCombi.map(tool => config.getDouble(s"$datasetName.$tool.precision"))
    org.apache.spark.mllib.linalg.Vectors.dense(weights.toArray)
  }

  def evalPredictionAndLabels(predictionAndLabels: RDD[(Double, Double)]) = {
    val outcomeCounts: Map[(Double, Double), Long] = predictionAndLabels.countByValue()

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

    val totalData = predictionAndLabels.count()

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
    testData
  }

  private def prepareDataToLIBSVM(session: SparkSession, dataDF: DataFrame): DataFrame = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, toolsForLinearCombi: _*)

    val data: DataFrame = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features = Vectors.dense(toolsVals)
      (label, features)
    }).toDF("label", "features")

    data
  }

  private def prepareDataToLabeledPoints(session: SparkSession, dataDF: DataFrame): RDD[LabeledPoint] = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, toolsForLinearCombi: _*)

    val data: RDD[LabeledPoint] = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      LabeledPoint(label, features)
    }).rdd

    data
  }

  def createModelFormula(intercept: Double, coefficients: Array[Double]): String = {
    var i = 0
    val function = coefficients.map(c => {
      val idx = {
        i += 1;
        i;
      }

      val coef = round(c)
      if (coef < 0) s"(${coef})t_{$idx}" else s"${coef}t_{$idx}"
    }).mkString(" + ")
    //s"""P(err)=\\frac{1}{1+\\exp ^{-($modelIntercept+$function)}}"""
    s""" z=${round(intercept)}+$function """
  }
}

object LinearFunctionPlayground {
  def main(args: Array[String]): Unit = {

    val linearFunction = new LinearFunction()
    val datasetName = "hosp"
    println(s"dataset: $datasetName")
    linearFunction.onDatasetName(datasetName)
    linearFunction.onTools(Seq("exists-2", "exists-4", "exists-5"))
    val evalLinearFunc = linearFunction.learnWithLBFGS()
    //val evalLinearFunc = linearFunction.learn()
    evalLinearFunc.printResult(s"final eval for $datasetName")
  }
}
