package de.model.naive.bayes

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.FormatUtil
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by visenger on 20/03/17.
  */
class BernoulliNaiveBayes extends ExperimentsCommonConfig {
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

  def run(session: SparkSession): Eval = {

    val trainDF = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*)
    val testDF = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*)

    val trainLabeledPoints = FormatUtil.prepareDataToLabeledPoints(session, trainDF, toolsForLinearCombi)
    val testLabeledPoints = FormatUtil.prepareDataToLabeledPoints(session, testDF, toolsForLinearCombi)

    val naiveBayesModel = NaiveBayes.train(trainLabeledPoints, lambda = 1.0, modelType = "bernoulli")

    val predictionAndLabels: RDD[(Double, Double)] = testLabeledPoints.map {
      case LabeledPoint(label, features) =>
        val prediction = naiveBayesModel.predict(features)
        (prediction, label)
    }

    val evalTestData: Eval = F1.evalPredictionAndLabels(predictionAndLabels)
    //    println(s"Naive Bayes for: $datasetName")
    //    println(evalTestData)

    evalTestData
  }


}


object BernoulliNaiveBayesRunner extends ExperimentsCommonConfig {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("NAIVE-BAYES") {
      session => {

        process_data {
          data => {

            val datasetName = data._1
            val trainDataPath = data._2._1
            val testDataPath = data._2._2

            val trainDF = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*)
            val testDF = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*)

            val trainLabeledPoints = FormatUtil.prepareDataToLabeledPoints(session, trainDF, FullResult.tools)
            val testLabeledPoints = FormatUtil.prepareDataToLabeledPoints(session, testDF, FullResult.tools)

            val naiveBayesModel = NaiveBayes.train(trainLabeledPoints, lambda = 1.0, modelType = "bernoulli")

            val predictionAndLabels: RDD[(Double, Double)] = testLabeledPoints.map {
              case LabeledPoint(label, features) =>
                val prediction = naiveBayesModel.predict(features)
                (prediction, label)
            }

            val evalTestData: Eval = F1.evalPredictionAndLabels(predictionAndLabels)
            println(s"Naive Bayes for: $datasetName")
            println(evalTestData)

          }
        }

      }
    }
  }


}
