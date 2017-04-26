package de.experiments.models.combinator

import de.evaluation.f1.{F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.logistic.regression.ModelData
import de.model.util.FormatUtil
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

/**
  * Created by visenger on 24/03/17.
  */
class ModelsCombiner {

}

object ModelsCombinerStrategy extends ExperimentsCommonConfig {

  val unionall = "unionAll"
  val minKCol = "minK"
  //val maxK = "maxK"
  //  val majorVote = "major"
  val naiveBayesCol = "naive-bayes"
  val dtCol = "decision-tree"
  val logRegrCol = "log-regr"
  val predictCol = "prediction"
  val nNetworksCol = "n-networks"

  def main(args: Array[String]): Unit = {
    //Timer.measureRuntime(() => runBagging())
    // Timer.measureRuntime(() => runCombineClassifiers())
    //    Timer.measureRuntime(() => runCombineTools())
    runBagging()
    runStacking()
    runCombineTools()
  }

  def getDecisionTreeModels(trainSamples: Seq[RDD[LabeledPoint]], tools: Seq[String]): Seq[DecisionTreeModel] = {
    val numClasses = 2
    val toolsNum = tools.size
    val categoricalFeaturesInfo = (0 until toolsNum).map(attr => attr -> numClasses).toMap // Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2, 6 -> 2, 7 -> 2)
    //        val impurity = "entropy"
    val impurity = "gini"
    val maxDepth = toolsNum
    val maxBins = 32

    val decisionTreeModels: Seq[DecisionTreeModel] = trainSamples.map(sample => {
      val decisionTreeModel: DecisionTreeModel = DecisionTree.trainClassifier(sample, numClasses, categoricalFeaturesInfo,
        impurity, maxDepth, maxBins)
      decisionTreeModel
    })
    decisionTreeModels
  }

  def runBagging(): Unit = {

    SparkLOAN.withSparkSession("BAGGING") {
      session => {

        process_data {
          data => {
            val datasetName = data._1
            val trainFile = data._2._1
            val testFile = data._2._2

            val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
            val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

            val allColumns = FullResult.tools
            val train = FormatUtil.prepareDataToLabeledPoints(session, trainDF, allColumns)

            val Array(_, train1) = train.randomSplit(Array(0.7, 0.3), seed = 123L)
            val Array(_, train2) = train.randomSplit(Array(0.7, 0.3), seed = 23L)
            val Array(_, train3) = train.randomSplit(Array(0.7, 0.3), seed = 593L)
            val Array(_, train4) = train.randomSplit(Array(0.7, 0.3), seed = 941L)
            val Array(_, train5) = train.randomSplit(Array(0.7, 0.3), seed = 3L)
            val Array(_, train6) = train.randomSplit(Array(0.7, 0.3), seed = 623L)

            val trainSamples = Seq(train1, train2, train3, train4, train5, train6)

            val Array(model1, model2, model3, model4, model5, model6) = getDecisionTreeModels(trainSamples, allColumns).toArray

            val bagging1 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model1.predict(features) }
            val bagging2 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model2.predict(features) }
            val bagging3 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model3.predict(features) }
            val bagging4 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model4.predict(features) }
            val bagging5 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model5.predict(features) }
            val bagging6 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model6.predict(features) }


            val test = FormatUtil.prepareToolsDataToLabPointsDF(session, testDF, allColumns)
            val featuresCol = "features"
            val baggingDF = test
              .withColumn(s"model-1", bagging1(test(featuresCol)))
              .withColumn(s"model-2", bagging2(test(featuresCol)))
              .withColumn(s"model-3", bagging3(test(featuresCol)))
              .withColumn(s"model-4", bagging4(test(featuresCol)))
              .withColumn(s"model-5", bagging5(test(featuresCol)))
              .withColumn(s"model-6", bagging6(test(featuresCol)))

            // Majority wins
            val majorityVoter = udf { (tools: mutable.WrappedArray[Double]) => {
              val total = tools.length
              val sum1 = tools.count(_ == 1.0)
              val sum0 = total - sum1
              val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
              errorDecision
            }
            }

            //todo: make decision rule more sophisticated -> weight classifiers

            val allModelsColumns = (1 to trainSamples.size).map(id => baggingDF(s"model-$id"))

            val majorityCol = "majority-vote"
            val majorityDF = baggingDF
              .withColumn(majorityCol, majorityVoter(array(allModelsColumns: _*)))
              .select(FullResult.label, majorityCol)

            val predictAndLabelMajority = FormatUtil.getPredictionAndLabel(majorityDF, majorityCol)
            val evalMajority = F1.evalPredictionAndLabels(predictAndLabelMajority)
            evalMajority.printResult(s"BAGGING (DT): $datasetName majority vote for ${allColumns.mkString(", ")}")


          }
        }


      }
    }

  }


  def runStacking(): Unit = {

    SparkLOAN.withSparkSession("MODELS-COMBINER") {
      session => {

        import session.implicits._

        process_data { data => {
          val dataSetName = data._1
          val trainFile = data._2._1
          val testFile = data._2._2

          println(s"STACKING: START PROCESSING $dataSetName")

          val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
          val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

          val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
          val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

          val allTools = FullResult.tools
          val trainLabeledPointsTools: RDD[LabeledPoint] = FormatUtil
            .prepareDataToLabeledPoints(session, trainDF, allTools)

          val bayesModel = NaiveBayes.train(trainLabeledPointsTools, lambda = 1.0, modelType = "bernoulli")
          val (bestLogRegrData, bestLogRegModel) = getBestModel(maxPrecision, maxRecall, trainLabeledPointsTools, trainLabeledPointsTools)

          //start: decision tree
          val numClasses = 2
          val toolsNum = allTools.size
          val categoricalFeaturesInfo = (0 until toolsNum)
            .map(attr => attr -> numClasses).toMap // Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2, 6 -> 2, 7 -> 2)
          val impurity = "gini"
          val maxDepth = toolsNum
          val maxBins = 32

          val decisionTreeModel: DecisionTreeModel = DecisionTree
            .trainClassifier(trainLabeledPointsTools, numClasses, categoricalFeaturesInfo,
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
            val sum = tools.numNonzeros
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


          var allClassifiers = testLabeledPointsTools
            .withColumn(dtCol, predictByDT(testLabeledPointsTools(features)))
            .withColumn(naiveBayesCol, predictByBayes(testLabeledPointsTools(features)))
            .withColumn(logRegrCol, predictByLogRegr(testLabeledPointsTools(features)))
            .withColumn(unionall, minKTools(lit(1), testLabeledPointsTools(features)))
            .withColumn(minKCol, minKTools(lit(toolsNum), testLabeledPointsTools(features)))
            .select(rowId, FullResult.label, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol)
            .toDF()

          //start:neural networks
          val nextLayer = toolsNum + 1
          val layers = Array[Int](toolsNum, nextLayer, toolsNum, numClasses)
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


          allClassifiers = allClassifiers
            .join(nnPrediction, rowId)
            .withColumnRenamed(predictCol, nNetworksCol)
            .select(rowId, FullResult.label, nNetworksCol, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol)


          //all possible combinations of errors classification and their counts
          val allResults = allClassifiers
            .rdd
            .map(row => {
              //val rowId = row.getLong(0)
              val label = row.getDouble(1)
              val nn = row.getDouble(2)
              val dt = row.getDouble(3)
              val bayes = row.getDouble(4)
              val logregr = row.getDouble(5)
              val unionAll = row.getDouble(6)
              val minK = row.getDouble(7)
              (label, nn, dt, bayes, logregr, unionAll, minK)
            })

          val countByValue = allResults.countByValue()

          println(s"label, nn, dt, nb, logreg, unionall, minK: count")
          countByValue.foreach(entry => {
            println(s"(${entry._1._1.toInt}, ${entry._1._2.toInt}, ${entry._1._3.toInt}, ${entry._1._4.toInt}, ${entry._1._5.toInt}, ${entry._1._6.toInt}, ${entry._1._7.toInt}) : ${entry._2}")
          })

          //F1 for all classifiers performed on tools result:
          val predAndLabelNN = FormatUtil.getPredictionAndLabel(allClassifiers, nNetworksCol)
          val evalNN = F1.evalPredictionAndLabels(predAndLabelNN)
          evalNN.printResult("Neural Networks")

          val predictionAndLabelDT = FormatUtil.getPredictionAndLabel(allClassifiers, dtCol)
          val evalDTrees = F1.evalPredictionAndLabels(predictionAndLabelDT)
          evalDTrees.printResult("decision trees")

          val predAndLabelLogReg = FormatUtil.getPredictionAndLabel(allClassifiers, logRegrCol)
          val evalLogReg = F1.evalPredictionAndLabels(predAndLabelLogReg)
          evalLogReg.printResult("logistic regression")

          val predictionAndLabelNB = FormatUtil.getPredictionAndLabel(allClassifiers, naiveBayesCol)
          val evalNB = F1.evalPredictionAndLabels(predictionAndLabelNB)
          evalNB.printResult("naive bayes")


          //Now combine all classifiers:
          println(s"COMBINE CLASSIFIERS")

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
            allClassifiers(minKCol))

          val finalCombiCol = "final-combi"

          (1 to clColumns.length).foreach(k => {
            val finalResult: DataFrame =
              allClassifiers
                .withColumn(finalCombiCol, minK(lit(k), array(clColumns: _*)))

            val predictionAndLabel = FormatUtil.getPredictionAndLabel(finalResult, finalCombiCol)
            val eval = F1.evalPredictionAndLabels(predictionAndLabel)
            eval.printResult(s"min-$k combination of nn, dt, nb, logreg, unionall, min5")
          })

          val maxKClassifiers = udf { (k: Int, tools: mutable.WrappedArray[Double]) => {
            val sum = tools.count(_ == 1.0)
            val errorDecision = if (1 to k contains sum) 1.0 else 0.0
            errorDecision
          }
          }

          // Majority wins
          val majorityVoter = udf { (tools: mutable.WrappedArray[Double]) => {
            val total = tools.length
            val sum1 = tools.count(_ == 1.0)
            var sum0 = total - sum1
            val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
            errorDecision
          }
          }

          val majorityVoterCol = "majority-voter"
          val majorityVoterStrategy = allClassifiers
            .withColumn(majorityVoterCol, majorityVoter(array(clColumns: _*)))
            .select(FullResult.label, majorityVoterCol)
            .toDF()

          val majorityVotersPredAndLabels = FormatUtil.getPredictionAndLabel(majorityVoterStrategy, majorityVoterCol)
          val majorityEval = F1.evalPredictionAndLabels(majorityVotersPredAndLabels)
          majorityEval.printResult("BAGGING: majority voters")
          //end: Majority wins


          val allClassifiersCols: Seq[String] = Array(nNetworksCol, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol).toSeq

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
          val featuresNumber = allClassifiersCols.size

          val toolsFeaturesInfo: Map[Int, Int] = (0 until featuresNumber).map(attr => attr -> 2).toMap // Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2)
          val mDepth = featuresNumber

          val model = DecisionTree.trainClassifier(trainClassifiers, numClasses, toolsFeaturesInfo,
            impurity, mDepth, maxBins)

          val predictionAndLabel = testClassifiers.map { point =>
            val prediction = model.predict(point.features)
            (prediction, point.label)
          }

          val evalDT = F1.evalPredictionAndLabels(predictionAndLabel)
          evalDT.printResult(s"decision tree combi based on $impurity")

        }
        }
      }
    }
  }


  def runCombineTools(): Unit = {

    SparkLOAN.withSparkSession("MODELS-COMBINER") {

      session => {

        process_data {
          data => {

            val dataSetName = data._1
            println(s"TOOLS COMBINATION: processing: $dataSetName")

            val trainFile = data._2._1
            val testFile = data._2._2
            //
            //            val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
            //            val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

            import org.apache.spark.sql.functions._

            val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
            val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

            val minK = udf { (k: Int, tools: mutable.WrappedArray[String]) => {
              val sum = tools.map(t => t.toInt).count(_ == 1)
              val errorDecision = if (sum >= k) "1" else "0"
              errorDecision
            }
            }
            /*see: Spark UDF with varargs: http://stackoverflow.com/questions/33151866/spark-udf-with-varargs?rq=1*/

            //            val toolsCols = FullResult.tools.map(t => trainDF(t)).toArray
            //            val k = 1
            //            val extendedTrainDF = trainDF
            //              .withColumn(unionall, minK(lit(k), array(toolsCols: _*)))
            //              .withColumn("min2", minK(lit(2), array(toolsCols: _*)))
            //              .withColumn("min3", minK(lit(3), array(toolsCols: _*)))
            //              .withColumn("min4", minK(lit(4), array(toolsCols: _*)))
            //              .withColumn("min5", minK(lit(5), array(toolsCols: _*)))
            //              .withColumn("min6", minK(lit(6), array(toolsCols: _*)))
            //              .withColumn("min7", minK(lit(7), array(toolsCols: _*)))
            //              .withColumn(min8, minK(lit(8), array(toolsCols: _*)))
            //              .toDF()

            //            val testToolsCols = FullResult.tools.map(t => testDF(t)).toArray
            //            val extendedTestDF = testDF
            //              .withColumn(unionall, minK(lit(k), array(testToolsCols: _*)))
            //              .withColumn("min2", minK(lit(2), array(testToolsCols: _*)))
            //              .withColumn("min3", minK(lit(3), array(testToolsCols: _*)))
            //              .withColumn("min4", minK(lit(4), array(testToolsCols: _*)))
            //              .withColumn("min5", minK(lit(5), array(testToolsCols: _*)))
            //              //              .withColumn("min6", minK(lit(6), array(testToolsCols: _*)))
            //              //              .withColumn("min7", minK(lit(7), array(testToolsCols: _*)))
            //              //              .withColumn(min8, minK(lit(8), array(testToolsCols: _*)))
            //              .toDF()


            val features = FullResult.tools
            println(s"aggregation running for: ${features.map(getName(_)).mkString(", ")}")
            //            val trainLabeledPoints = FormatUtil.prepareDataToLabeledPoints(session, extendedTrainDF, features)
            //            val testLabeledPoints: RDD[LabeledPoint] = FormatUtil.prepareDataToLabeledPoints(session, extendedTestDF, features)

            //            val naiveBayesModel = NaiveBayes.train(trainLabeledPoints, lambda = 1.0, modelType = "bernoulli")
            //
            //            val predictionAndLabels: RDD[(Double, Double)] = testLabeledPoints.map {
            //              case LabeledPoint(label, features) =>
            //                val prediction = naiveBayesModel.predict(features)
            //                (prediction, label)
            //            }
            //
            //            val evalTestData: Eval = F1.evalPredictionAndLabels(predictionAndLabels)
            //            evalTestData.printResult(s"naive bayes for ${features.map(getName(_)).mkString(", ")} ")

            //            val (bestModelData, _) = getBestModel(maxPrecision, maxRecall, trainLabeledPoints, testLabeledPoints)
            //
            //            println(s"Log regression :${bestModelData}")

            // Majority wins
            val majorityVoter = udf { tools: org.apache.spark.mllib.linalg.Vector => {
              val total = tools.size
              val sum1 = tools.numNonzeros
              val sum0 = total - sum1
              val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
              errorDecision
            }
            }

            val testLabPointsDF: DataFrame = FormatUtil.prepareToolsDataToLabPointsDF(session, trainDF, features)
            val majorityVoterCol = "majority-voter"
            val majorityVoterStrategy = testLabPointsDF
              .withColumn(majorityVoterCol, majorityVoter(testLabPointsDF("features")))
              .select(FullResult.label, majorityVoterCol)
              .toDF()

            val majorityVotersPredAndLabels = FormatUtil.getPredictionAndLabel(majorityVoterStrategy, majorityVoterCol)
            val majorityEval = F1.evalPredictionAndLabels(majorityVotersPredAndLabels)
            majorityEval.printResult("MAJORITY WINS (tools)")


          }
        }

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
