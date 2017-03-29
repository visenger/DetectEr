package de.experiments.models.combinator

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.logistic.regression.ModelData
import de.model.util.FormatUtil
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

/**
  * Created by visenger on 24/03/17.
  */
class ModelsCombiner {

}

object ModelsCombinerStrategy extends ExperimentsCommonConfig {
  val unionall = "unionAll"
  val min8 = "min8"

  def main(args: Array[String]): Unit = {
    runCombineClassifiers()
  }

  def runCombineClassifiers(): Unit = {

    SparkLOAN.withSparkSession("MODELS-COMBINER") {
      session => {

        import org.apache.spark.sql.functions._
        import session.implicits._

        val dataSetName = "ext.blackoak"
        val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
        val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

        val maximumF1 = experimentsConf.getDouble(s"${dataSetName}.max.F1")
        val minimumF1 = experimentsConf.getDouble(s"${dataSetName}.min.F1")


        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        val testDF = DataSetCreator.createFrame(session, extBlackoakTestFile, FullResult.schema: _*)


        val trainLabeledPointsTools = FormatUtil
          .prepareDataToLabeledPoints(session, trainDF, FullResult.tools)

        val bayesModel = NaiveBayes.train(trainLabeledPointsTools, lambda = 1.0, modelType = "bernoulli")
        val (bestLogRegrData, bestLogRegModel) = getBestModel(maxPrecision, maxRecall, trainLabeledPointsTools, trainLabeledPointsTools)

        //start: decision tree
        val numClasses = 2
        val categoricalFeaturesInfo = Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2, 6 -> 2, 7 -> 2)
        //        val impurity = "entropy"
        val impurity = "gini"
        val maxDepth = 8
        val maxBins = 32

        val decisionTreeModel = DecisionTree.trainClassifier(trainLabeledPointsTools, numClasses, categoricalFeaturesInfo,
          impurity, maxDepth, maxBins)
        //finish: decision tree
        val predictByDT = udf { features: org.apache.spark.mllib.linalg.Vector => decisionTreeModel.predict(features) }

        val predictByLogRegr = udf { features: org.apache.spark.mllib.linalg.Vector => {
          bestLogRegModel.setThreshold(bestLogRegrData.bestThreshold)
          bestLogRegModel.predict(features)
        }
        }
        val predictByBayes = udf { features: org.apache.spark.mllib.linalg.Vector => bayesModel.predict(features) }
        val minKTools = udf { (k: Int, tools: org.apache.spark.mllib.linalg.Vector) => {
          val sum = tools.toArray.sum
          val errorDecision = if (sum >= k) 1.0 else 0.0
          errorDecision
        }
        }

        val features = "features"
        val rowId = "row-id"

        val testLabeledPointsTools: DataFrame = FormatUtil
          .prepareDataToLabeledPoints(session, testDF, FullResult.tools)
          .toDF(FullResult.label, features)
          .withColumn(rowId, monotonically_increasing_id())

        val naiveBayesCol = "naive-bayes"
        val dtCol = "naive-bayes"
        val logRegrCol = "log-regr"
        val predictCol = "prediction"


        //TODO: add min-2 to min-7 classifiers
        var allClassifiers = testLabeledPointsTools
          .withColumn(dtCol, predictByDT(testLabeledPointsTools(features)))
          .withColumn(naiveBayesCol, predictByBayes(testLabeledPointsTools(features)))
          .withColumn(logRegrCol, predictByLogRegr(testLabeledPointsTools(features)))
          .withColumn(unionall, minKTools(lit(1), testLabeledPointsTools(features)))
          .withColumn(min8, minKTools(lit(8), testLabeledPointsTools(features)))
          .select(rowId, FullResult.label, dtCol, naiveBayesCol, logRegrCol, unionall, min8)
          .toDF()

        //start:neural networks
        val layers = Array[Int](8, 9, 8, 2)
        val trainer = new MultilayerPerceptronClassifier()
          .setLayers(layers)
          .setBlockSize(128)
          .setSeed(1234L)
          .setMaxIter(100)
        val nnTrain = FormatUtil.prepareDataToLIBSVM(session, trainDF, FullResult.tools)
        val networkModel = trainer.fit(nnTrain)
        val testWithRowId = testDF.withColumn(rowId, monotonically_increasing_id())
        val nnTest = FormatUtil.prepareDataWithRowIdToLIBSVM(session, testWithRowId, FullResult.tools)
        val result = networkModel.transform(nnTest)
        val nnPrediction = result.select(rowId, predictCol)
        //end:neural networks

        val nNetworksCol = "n-networks"
        allClassifiers = allClassifiers
          .join(nnPrediction, rowId)
          .withColumnRenamed(predictCol, nNetworksCol)
          .select(rowId, FullResult.label, nNetworksCol, dtCol, naiveBayesCol, logRegrCol, unionall, min8)


        /*   //all possible combinations of errors classification and their counts
           val allResults: RDD[(Double, Double, Double, Double, Double, Double, Double)] = allClassifiers
             .rdd
             .map(row => {
               //val rowId = row.getLong(0)
               val label = row.getDouble(1)
               val nn = row.getDouble(2)
               val dt = row.getDouble(3)
               val bayes = row.getDouble(4)
               val logregr = row.getDouble(5)
               val unionAll = row.getDouble(6)
               val min8 = row.getDouble(6)
               (label, nn, dt, bayes, logregr, unionAll, min8)
             })

           val countByValue = allResults.countByValue()

           println(s"label, nn, dt, naive bayes, log regression, unionall, min8 : count")
           countByValue.foreach(entry => {
             println(s"(${entry._1._1.toInt}, ${entry._1._2.toInt}, ${entry._1._3.toInt}, ${entry._1._4.toInt}, ${entry._1._5.toInt}, ${entry._1._6.toInt}, ${entry._1._7.toInt}) : ${entry._2}")
           })*/

        val minK = udf { (k: Int, tools: mutable.WrappedArray[Double]) => {
          val sum = tools.count(_ == 1.0)
          val errorDecision = if (sum >= k) 1.0 else 0.0
          errorDecision
        }
        }

        val clColumns: Array[Column] = Array(
          allClassifiers(nNetworksCol),
          allClassifiers(dtCol),
          allClassifiers(naiveBayesCol),
          allClassifiers(logRegrCol),
          allClassifiers(unionall),
          allClassifiers(min8))

        val finalCombiCol = "final-combi"
        (1 to clColumns.length).foreach(k => {
          val finalResult: DataFrame =
            allClassifiers
              .withColumn(finalCombiCol, minK(lit(k), array(clColumns: _*)))
              .select(FullResult.label, finalCombiCol)
              .toDF()

          val predictionAndLabel = finalResult.rdd.map(row => {
            val label = row.getDouble(0)
            val prediction = row.getDouble(1)
            (prediction, label)
          })

          val eval = F1.evalPredictionAndLabels(predictionAndLabel)
          eval.printResult(s"min-$k combination of neural networks, decision tree, naive-bayes, log regression, unionall, min8:")
        })
        val allClassifiersCols = Array(nNetworksCol, dtCol, naiveBayesCol, logRegrCol, unionall, min8).toSeq

        val labeledPointsClassifiers = FormatUtil.prepareDoublesToLabeledPoints(session, allClassifiers, allClassifiersCols)

        val Array(trainClassifiers, testClassifiers) = labeledPointsClassifiers.randomSplit(Array(0.2, 0.8))

        //Logistic Regression for classifier combination:
        val (classiBestModelData, classiBestModel) =
        //todo: Achtung: we removed the max precision and max recall threshold
          getBestModel(0.99, 0.99, trainClassifiers, trainClassifiers)

        val predictByLogRegrCombi = udf { features: org.apache.spark.mllib.linalg.Vector => {
          classiBestModel.setThreshold(classiBestModelData.bestThreshold)
          classiBestModel.predict(features)
        }
        }

        val testClassifiersDF = testClassifiers.toDF()

        val logRegrOfAllCombi: RDD[(Double, Double)] = testClassifiersDF
          .withColumn("lr-of-combi", predictByLogRegrCombi(testClassifiersDF(features)))
          .select(FullResult.label, "lr-of-combi")
          .rdd.map(row => {
          val label = row.getDouble(0)
          val prediction = row.getDouble(1)
          (prediction, label)
        })

        val evalCombi = F1.evalPredictionAndLabels(logRegrOfAllCombi)
        evalCombi.printResult("linear combi of all")

        //Naive Bayes for classifier combination:
        val naiveBayesModel = NaiveBayes.train(trainClassifiers, lambda = 1.0, modelType = "bernoulli")
        val predictByNaiveBayes = udf { features: org.apache.spark.mllib.linalg.Vector => {
          naiveBayesModel.predict(features)
        }
        }

        val bayesOfAllCombi: RDD[(Double, Double)] = testClassifiersDF
          .withColumn("bayes-of-combi", predictByNaiveBayes(testClassifiersDF(features)))
          .select(FullResult.label, "bayes-of-combi")
          .rdd.map(row => {
          val label = row.getDouble(0)
          val prediction = row.getDouble(1)
          (prediction, label)
        })


        val evalCombiBayes = F1.evalPredictionAndLabels(bayesOfAllCombi)
        evalCombiBayes.printResult("bayes combi of all")

        //DecisionTree for classifier combi;
        val toolsFeaturesInfo = Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2)
        val impurityFinal = "entropy"
        val mDepth = 6

        val model = DecisionTree.trainClassifier(trainClassifiers, numClasses, toolsFeaturesInfo,
          impurityFinal, mDepth, maxBins)

        val predictionAndLabel = testClassifiers.map { point =>
          val prediction = model.predict(point.features)
          (prediction, point.label)
        }

        val evalDT = F1.evalPredictionAndLabels(predictionAndLabel)
        evalDT.printResult(s"decision tree combi based on $impurity")


        //todo: rules!

        //allClassifiers.show()


      }
    }

  }


  def runCombineTools(): Unit = {

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
          .withColumn(unionall, minK(lit(1), array(toolsCols: _*)))
          .withColumn("min2", minK(lit(2), array(toolsCols: _*)))
          .withColumn("min3", minK(lit(3), array(toolsCols: _*)))
          .withColumn("min4", minK(lit(4), array(toolsCols: _*)))
          .withColumn("min5", minK(lit(5), array(toolsCols: _*)))
          .withColumn("min6", minK(lit(6), array(toolsCols: _*)))
          .withColumn("min7", minK(lit(7), array(toolsCols: _*)))
          .withColumn(min8, minK(lit(8), array(toolsCols: _*)))
          .toDF()

        val testToolsCols = FullResult.tools.map(t => testDF(t)).toArray
        val extendedTestDF = testDF
          .withColumn(unionall, minK(lit(1), array(testToolsCols: _*)))
          .withColumn("min2", minK(lit(2), array(testToolsCols: _*)))
          .withColumn("min3", minK(lit(3), array(testToolsCols: _*)))
          .withColumn("min4", minK(lit(4), array(testToolsCols: _*)))
          .withColumn("min5", minK(lit(5), array(testToolsCols: _*)))
          .withColumn("min6", minK(lit(6), array(testToolsCols: _*)))
          .withColumn("min7", minK(lit(7), array(testToolsCols: _*)))
          .withColumn(min8, minK(lit(8), array(testToolsCols: _*)))
          .toDF()

        val features = FullResult.tools ++ Seq("min2", "min3", "min4", "min5", "min6", "min7", min8, unionall)
        val trainLabeledPoints = FormatUtil.prepareDataToLabeledPoints(session, extendedTrainDF, features)
        val testLabeledPoints: RDD[LabeledPoint] = FormatUtil.prepareDataToLabeledPoints(session, extendedTestDF, features)

        val naiveBayesModel = NaiveBayes.train(trainLabeledPoints, lambda = 1.0, modelType = "bernoulli")

        val predictionAndLabels: RDD[(Double, Double)] = testLabeledPoints.map {
          case LabeledPoint(label, features) =>
            val prediction = naiveBayesModel.predict(features)
            (prediction, label)
        }

        val evalTestData: Eval = F1.evalPredictionAndLabels(predictionAndLabels)
        evalTestData.printResult("naive bayes (all tools and all minK's): ")

        val (bestModelData, bestModel) = getBestModel(maxPrecision, maxRecall, trainLabeledPoints, testLabeledPoints)

        println(s"Log regression (all tools and all minK's):${bestModelData}")

      }
    }

  }


  private def getBestModel(maxPrecision: Double,
                           maxRecall: Double,
                           train: RDD[LabeledPoint],
                           test: RDD[LabeledPoint]): (ModelData, LogisticRegressionModel)

  = {

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
      val predictionAndLabels: RDD[(Double, Double)] = test.map { case LabeledPoint(label, features) =>
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
