package de.model.logistic.regression

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{Eval, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.NumbersUtil.round
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


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

  def onDatasetName(data: String): this.type = {
    val dataTrainFile = s"${data.toLowerCase}.experiments.train.file"
    trainDataPath = config.getString(dataTrainFile)

    val dataTestFile = s"${data.toLowerCase}.experiments.test.file"
    testDataPath = config.getString(dataTestFile)

    maxPrecision = config.getDouble(s"${data.toLowerCase}.max.precision")
    maxRecall = config.getDouble(s"${data.toLowerCase}.max.recall")

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

  def evaluateLinearCombi(session: SparkSession): Eval = {
    val dataDF: DataFrame = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*)
    val train: DataFrame = prepareDataToLIBSVM(session, dataDF)

    //todo: magic numbers!
    val allModels: Seq[(LogisticRegressionModel, Double)] = (1 to 15)
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

    val elasticNetParam = ind % 5 match {
      case 1 => 0.2 //0.01
      case 2 => 0.8
      case 3 => 0.999
      case 4 => 0.01
      case 0 => 0.0
    }

    val regParam = 0.0
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
      .where(preci("precision") <= maxPrecision && recall("recall") <= maxRecall)

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

    //println(trainDataInfo)
    (model, round(bestF1))
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
    function
    //s"""P(err)=\\frac{1}{1+\\exp ^{-($modelIntercept+$function)}}"""
    s""" z=${round(intercept)}+$function """
  }
}

object LinearFunctionPlayground {
  def main(args: Array[String]): Unit = {

    val linearFunction = new LinearFunction()
    val datasetName = "salaries"
    println(s"dataset: $datasetName")
    linearFunction.onDatasetName(datasetName)
    linearFunction.onTools(Seq("exists-2", "exists-4"))
    val evalLinearFunc: Eval = linearFunction.learn()
  }
}
