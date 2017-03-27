package de.experiments.models.combinator

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.logistic.regression.ModelData
import de.model.util.FormatUtil
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by visenger on 24/03/17.
  */
class ModelsCombiner {

}

object ModelsCombinerStrategy extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    run()
  }

  def run(): Unit = {

    SparkLOAN.withSparkSession("MODELS-COMBINER") {
      session => {
        val dataSetName = "ext.blackoak"
        val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
        val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

        val maximumF1 = experimentsConf.getDouble(s"${dataSetName}.max.F1")
        val minimumF1 = experimentsConf.getDouble(s"${dataSetName}.min.F1")

        import org.apache.spark.sql.functions._

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        val testDF = DataSetCreator.createFrame(session, extBlackoakTestFile, FullResult.schema: _*)

        val minK = udf { (k: Int, tools: mutable.WrappedArray[String]) => {
          val sum = tools.map(t => t.toInt).count(_ == 1)
          val errorDecision = if (sum >= k) "1" else "0"
          errorDecision
        }
        }

        /*see: Spark UDF with varargs: http://stackoverflow.com/questions/33151866/spark-udf-with-varargs?rq=1*/

        val toolsCols = FullResult.tools.map(t => trainDF(t)).toArray

        val extendedTrainDF = trainDF
          .withColumn("unionAll", minK(lit(1), array(toolsCols: _*)))
          .withColumn("min2", minK(lit(2), array(toolsCols: _*)))
          .withColumn("min3", minK(lit(3), array(toolsCols: _*)))
          .withColumn("min4", minK(lit(4), array(toolsCols: _*)))
          .withColumn("min5", minK(lit(5), array(toolsCols: _*)))
          .withColumn("min6", minK(lit(6), array(toolsCols: _*)))
          .withColumn("min7", minK(lit(7), array(toolsCols: _*)))
          .withColumn("min8", minK(lit(8), array(toolsCols: _*)))
          .toDF()

        val testToolsCols = FullResult.tools.map(t => testDF(t)).toArray
        val extendedTestDF = testDF
          .withColumn("unionAll", minK(lit(1), array(testToolsCols: _*)))
          .withColumn("min2", minK(lit(2), array(testToolsCols: _*)))
          .withColumn("min3", minK(lit(3), array(testToolsCols: _*)))
          .withColumn("min4", minK(lit(4), array(testToolsCols: _*)))
          .withColumn("min5", minK(lit(5), array(testToolsCols: _*)))
          .withColumn("min6", minK(lit(6), array(testToolsCols: _*)))
          .withColumn("min7", minK(lit(7), array(testToolsCols: _*)))
          .withColumn("min8", minK(lit(8), array(testToolsCols: _*)))
          .toDF()

        val features = FullResult.tools ++ Seq("min2", "min3", "min4", "min5", "min6", "min7", "min8", "unionAll")
        val trainLabeledPoints = FormatUtil.prepareDataToLabeledPoints(session, extendedTrainDF, features)
        val testLabeledPoints: RDD[LabeledPoint] = FormatUtil.prepareDataToLabeledPoints(session, extendedTestDF, features)

        val naiveBayesModel = NaiveBayes.train(trainLabeledPoints, lambda = 1.0, modelType = "bernoulli")

        val predictionAndLabels: RDD[(Double, Double)] = testLabeledPoints.map {
          case LabeledPoint(label, features) =>
            val prediction = naiveBayesModel.predict(features)
            (prediction, label)
        }

        val evalTestData: Eval = F1.evalPredictionAndLabels(predictionAndLabels)
        evalTestData.printResult("naive bayes")

        val (bestModelData, bestModel) = getBestModel(maxPrecision, maxRecall, trainLabeledPoints, testLabeledPoints)

        println(bestModelData)

      }
    }

  }

  private def getBestModel(maxPrecision: Double,
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
      val predictionAndLabels: RDD[(Double, Double)] = train.map { case LabeledPoint(label, features) =>
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


}
