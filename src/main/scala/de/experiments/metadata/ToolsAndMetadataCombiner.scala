package de.experiments.metadata

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{SparkLOAN, Timer}
import de.experiments.models.combinator.{Bagging, Stacking}
import de.model.logistic.regression.ModelData
import de.model.util.FormatUtil
import de.wrangling.WranglingDatasetsToMetadata
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, udf}

import scala.collection.mutable

/**
  * Created by visenger on 11/04/17.
  */
class ToolsAndMetadataCombiner {

}

object ToolsAndMetadataCombinerRunner {

  def main(args: Array[String]): Unit = {
    println(s" RUNNING EXPERIMENTS ON TOOLS & METADATA COMBINATION BY USING SEVERAL STRATEGIES")

    val datasets = Seq("blackoak", "hosp", "salaries", "flights")


    datasets.foreach(datasetName => {
      Timer.measureRuntime {
        () => runBaggingOnDecisionTreeForMetadata(datasetName)
      }
    })




    datasets.foreach(datasetName => {
      Timer.measureRuntime {
        () => runStackingForMetadata(datasetName)
      }
    })

  }


//  def runStackingFor(datasetName: String): Unit = {
//    SparkLOAN.withSparkSession("STACKING-ON-METADATA") {
//      session => {
//        val stacking = new Stacking()
//        val evalStackingWithMetadata = stacking.onDataSetName(datasetName)
//          .useTools(FullResult.tools)
//          .performEnsambleLearningOnTools(session)
//        evalStackingWithMetadata.printResult(s"STACKING ON TOOLS ONLY FOR $datasetName ")
//
//      }
//    }
//  }

  def runStackingForMetadata(datasetName: String): Unit = {
    SparkLOAN.withSparkSession("STACKING-ON-METADATA") {
      session => {
        val stacking = new Stacking()
        val evalStackingWithMetadata = stacking
          .onDataSetName(datasetName)
          .useTools(FullResult.tools)
          .performEnsambleLearningOnToolsAndMetadata(session)
        evalStackingWithMetadata.printResult(s"STACKING ON TOOLS AND METADATA FOR $datasetName ")

      }
    }
  }

//  def runBaggingOnDecisionTreeFor(datasetName: String): Unit = {
  //    SparkLOAN.withSparkSession("METADATA") {
  //      session => {
  //        val bagging = new Bagging()
  //        val evalBaggingOnToolsAndMetadata: Eval = bagging.onDataSetName(datasetName)
  //          .useTools(FullResult.tools)
  //          .performEnsambleLearningOnTools(session)
  //
  //        evalBaggingOnToolsAndMetadata.printResult(s"BAGGING ON TOOLS ONLY FOR $datasetName")
  //
  //      }
  //    }
  //  }

  def runBaggingOnDecisionTreeForMetadata(datasetName: String): Unit = {
    SparkLOAN.withSparkSession("METADATA") {
      session => {
        val bagging = new Bagging()
        val evalBaggingOnToolsAndMetadata: Eval = bagging.onDataSetName(datasetName)
          .useTools(FullResult.tools)
          .performEnsambleLearningOnToolsAndMetadata(session)

        evalBaggingOnToolsAndMetadata.printResult(s"BAGGING ON TOOLS AND METADATA FOR $datasetName")

      }
    }
  }


  def runSingleDecisionTreeOn(datasetName: String): Unit = {
    //val datasetName = "hosp"
    SparkLOAN.withSparkSession("METADATA") {
      session => {
        import session.implicits._

        val (train, test) = new WranglingDatasetsToMetadata()
          .onDatasetName(datasetName)
          .createMetadataFeatures(session)

        val trainLabPointRDD = FormatUtil
          .prepareDFToLabeledPointRDD(session, train)

        //APPLY CLASSIFICATION
        val decisionTreeModel: DecisionTreeModel = getDecisionTreeModels(Seq(trainLabPointRDD), getFeaturesNumber(train)).head // DecisionTree          .trainClassifier(trainLabPointRDD, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

        val predictByDT = udf { features: org.apache.spark.mllib.linalg.Vector => decisionTreeModel.predict(features) }

        val testLabAndFeatures = FormatUtil
          .prepareDFToLabeledPointRDD(session, test)
          .toDF(FullResult.label, "features")

        val dtResult = testLabAndFeatures.withColumn("dt-prediction", predictByDT(testLabAndFeatures("features")))
        val dtPredictionAndLabel = FormatUtil.getPredictionAndLabel(dtResult, "dt-prediction")
        val dtEval = F1.evalPredictionAndLabels(dtPredictionAndLabel)
        dtEval.printResult(s"decision tree on $datasetName with metadata")
      }
    }
  }

  def runNeuralNetworksOn(datasetName: String): Unit = {
    //val datasetName = "hosp"
    SparkLOAN.withSparkSession("METADATA") {
      session => {

        val (train, test) = new WranglingDatasetsToMetadata()
          .onDatasetName(datasetName)
          .createMetadataFeatures(session)

        //        val trainLabPointRDD = FormatUtil
        //          .prepareDFToLabeledPointRDD(session, train)
        val labelAsDouble = udf {
          label: String => label.toDouble
        }
        //APPLY CLASSIFICATION
        //start:neural networks
        val numClasses = 2
        val layer = getFeaturesNumber(train)
        val nextLayer = layer + 1
        val layers = Array[Int](layer, nextLayer, layer, numClasses)
        val trainer = new MultilayerPerceptronClassifier()
          .setLayers(layers)
          .setBlockSize(128)
          .setSeed(1234L)
          .setMaxIter(100)

        val trainDF = train.withColumn("label-temp", labelAsDouble(train(FullResult.label)))
        val transformedTrainDF = trainDF.select("label-temp", "features").withColumnRenamed("label-temp", FullResult.label)

        val networkModel = trainer.fit(transformedTrainDF)

        val testDF = test.withColumn("label-temp", labelAsDouble(test(FullResult.label)))
        val transformedTestDF = testDF.select("label-temp", "features").withColumnRenamed("label-temp", FullResult.label)

        val result = networkModel.transform(transformedTestDF)
        val nnPrediction = result.select(FullResult.label, "prediction")
        //end:neural networks

        val nnPredictionAndLabel = FormatUtil.getPredictionAndLabel(nnPrediction, "prediction")
        val nnEval = F1.evalPredictionAndLabels(nnPredictionAndLabel)
        nnEval.printResult(s"neural network for $datasetName with metadata")


      }
    }
  }

  def getDecisionTreeModels(trainSamples: Seq[RDD[LabeledPoint]], toolsNum: Int): Seq[DecisionTreeModel] = {
    val numClasses = 2
    val categoricalFeaturesInfo = (0 until toolsNum)
      .map(attr => attr -> numClasses).toMap // Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2)
    //        val impurity = "entropy"
    val impurity = "gini"
    val maxDepth = 5
    // toolsNum
    val maxBins = 32

    val decisionTreeModels: Seq[DecisionTreeModel] = trainSamples.map(sample => {
      val decisionTreeModel: DecisionTreeModel = DecisionTree
        .trainClassifier(sample, numClasses, categoricalFeaturesInfo,
          impurity, maxDepth, maxBins)
      decisionTreeModel
    })
    decisionTreeModels
  }

  private def getFeaturesNumber(featuresDF: DataFrame): Int

  = {
    featuresDF.select("features").head().getAs[org.apache.spark.ml.linalg.Vector](0).size
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

  @Deprecated
  def _runStackingFor(datasetName: String): Unit = {
    SparkLOAN.withSparkSession("STACKING-ON-METADATA") {
      session => {
        import session.implicits._
        println(s"STACKING WITH METADATA ON $datasetName")
        val (train, test) = new WranglingDatasetsToMetadata()
          .onDatasetName(datasetName)
          .createMetadataFeatures(session)

        val featuresCol = "features"

        val trainLabPointRDD: RDD[LabeledPoint] = FormatUtil
          .prepareDFToLabeledPointRDD(session, train)

        val testLabAndFeatures: DataFrame = FormatUtil
          .prepareDFToLabeledPointRDD(session, test)
          .toDF(FullResult.label, featuresCol)

        val featuresNumber = getFeaturesNumber(train)
        val numClasses = 2

        //todo 1: nn, dt, nb
        // todo: update train data as you go.

        //start:neural networks
        val nextLayer = featuresNumber + 1
        val layers = Array[Int](featuresNumber, nextLayer, featuresNumber, numClasses)
        val trainer = new MultilayerPerceptronClassifier()
          .setLayers(layers)
          .setBlockSize(128)
          .setSeed(1234L)
          .setMaxIter(100)
        val trainDF = trainLabPointRDD.toDF(FullResult.label, featuresCol)
        val nnTrain: DataFrame = FormatUtil.convertVectors(session, trainDF, featuresCol)
        val networkModel = trainer.fit(nnTrain)
        val nnTest: DataFrame = FormatUtil.convertVectors(session, testLabAndFeatures, featuresCol)
        val result: DataFrame = networkModel.transform(nnTest)
        val resultTrain: DataFrame = networkModel.transform(nnTrain)
        val nnTrainPrediction = resultTrain.withColumnRenamed("prediction", "nn-prediction")
        val nnPrediction: DataFrame = result.withColumnRenamed("prediction", "nn-prediction")
        val nnPredictionAndLabel = FormatUtil.getPredictionAndLabel(nnPrediction, "nn-prediction")
        val nnEval = F1.evalPredictionAndLabels(nnPredictionAndLabel)
        nnEval.printResult(s"neural networks on $datasetName with metadata")
        //end:neural networks


        //start: decision tree:
        val decisionTreeModel: DecisionTreeModel = getDecisionTreeModels(Seq(trainLabPointRDD), featuresNumber).head

        val predictByDT = udf { features: org.apache.spark.ml.linalg.Vector => {
          val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
          decisionTreeModel.predict(transformedFeatures)
        }
        }

        val dtPrediction = nnPrediction.withColumn("dt-prediction", predictByDT(nnPrediction(featuresCol)))
        val dtTrainPrediction = nnTrainPrediction.withColumn("dt-prediction", predictByDT(nnTrainPrediction(featuresCol)))
        val dtPredictionAndLabel = FormatUtil.getPredictionAndLabel(dtPrediction, "dt-prediction")
        val dtEval = F1.evalPredictionAndLabels(dtPredictionAndLabel)
        dtEval.printResult(s"decision tree on $datasetName with metadata")
        //finish: decision tree

        //start: bayes
        val bayesModel = NaiveBayes.train(trainLabPointRDD, lambda = 1.0, modelType = "bernoulli")
        val predictByBayes = udf { features: org.apache.spark.ml.linalg.Vector => {
          val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
          bayesModel.predict(transformedFeatures)
        }
        }

        val nbPrediction = dtPrediction.withColumn("nb-prediction", predictByBayes(dtPrediction(featuresCol)))
        val nbTrainPrediction = dtTrainPrediction.withColumn("nb-prediction", predictByBayes(dtTrainPrediction(featuresCol)))
        val nbPredictionAndLabel = FormatUtil.getPredictionAndLabel(nbPrediction, "nb-prediction")
        val nbEval = F1.evalPredictionAndLabels(nbPredictionAndLabel)
        nbEval.printResult(s"naive bayes on $datasetName with metadata")
        //end: bayes


        //todo 2: meta classifier: logreg

        val assembler = new VectorAssembler()
          .setInputCols(Array("nn-prediction", "dt-prediction", "nb-prediction"))
          .setOutputCol("all-predictions")

        //Meta train
        val allTrainPrediction = assembler.transform(nbTrainPrediction).select(FullResult.label, "all-predictions")
        val allTrainClassifiers = allTrainPrediction.withColumnRenamed("all-predictions", featuresCol)

        //Train LogModel
        val trainLabeledRDD: RDD[LabeledPoint] = FormatUtil.prepareDFToLabeledPointRDD(session, allTrainClassifiers)
        val (modelData, lrModel) = getBestModel(1.0, 1.0, trainLabeledRDD, trainLabeledRDD)

        val logRegPredictor = udf { features: org.apache.spark.ml.linalg.Vector => {
          lrModel.setThreshold(modelData.bestThreshold)
          val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
          lrModel.predict(transformedFeatures)
        }
        }


        //Meta test
        val allPredictions = assembler.transform(nbPrediction).select(FullResult.label, "all-predictions")
        val allClassifiers = allPredictions.withColumnRenamed("all-predictions", featuresCol)
        val firstClassifiersDF = allClassifiers
          .select(FullResult.label, featuresCol)
          .toDF(FullResult.label, featuresCol)

        val lrPredictorDF = firstClassifiersDF
          .withColumn("final-predictor", logRegPredictor(firstClassifiersDF(featuresCol)))
        val lrPredictionAndLabel = FormatUtil.getPredictionAndLabel(lrPredictorDF, "final-predictor")
        val lrEval = F1.evalPredictionAndLabels(lrPredictionAndLabel)
        lrEval.printResult(s"STACKING RESULT FOR $datasetName")


      }
    }
  }

  @Deprecated
  def _runBaggingOnDecisionTreeFor(datasetName: String): Unit = {
    SparkLOAN.withSparkSession("METADATA") {
      session => {
        import session.implicits._

        val (train, test) = new WranglingDatasetsToMetadata()
          .onDatasetName(datasetName)
          .createMetadataFeatures(session)

        val featuresCol = "features"

        val trainLabPointRDD: RDD[LabeledPoint] = FormatUtil
          .prepareDFToLabeledPointRDD(session, train)

        val testLabAndFeatures: DataFrame = FormatUtil
          .prepareDFToLabeledPointRDD(session, test)
          .toDF(FullResult.label, featuresCol)

        //APPLY CLASSIFICATION
        //start: decision tree

        val Array(_, train1) = trainLabPointRDD.randomSplit(Array(0.5, 0.5), seed = 123L)
        val Array(train2, _) = trainLabPointRDD.randomSplit(Array(0.5, 0.5), seed = 23L)
        val Array(_, train3) = trainLabPointRDD.randomSplit(Array(0.5, 0.5), seed = 593L)
        val Array(train4, _) = trainLabPointRDD.randomSplit(Array(0.5, 0.5), seed = 941L)
        val Array(_, train5) = trainLabPointRDD.randomSplit(Array(0.5, 0.5), seed = 3L)
        val Array(train6, _) = trainLabPointRDD.randomSplit(Array(0.5, 0.5), seed = 623L)

        val trainSamples = Seq(train1, train2, train3, train4, train5, train6)

        val featuresSize = getFeaturesNumber(train)

        val Array(model1, model2, model3, model4, model5, model6) = getDecisionTreeModels(trainSamples, featuresSize).toArray

        val bagging1 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model1.predict(features) }
        val bagging2 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model2.predict(features) }
        val bagging3 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model3.predict(features) }
        val bagging4 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model4.predict(features) }
        val bagging5 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model5.predict(features) }
        val bagging6 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model6.predict(features) }


        val baggingDF = testLabAndFeatures
          .withColumn(s"model-1", bagging1(testLabAndFeatures(featuresCol)))
          .withColumn(s"model-2", bagging2(testLabAndFeatures(featuresCol)))
          .withColumn(s"model-3", bagging3(testLabAndFeatures(featuresCol)))
          .withColumn(s"model-4", bagging4(testLabAndFeatures(featuresCol)))
          .withColumn(s"model-5", bagging5(testLabAndFeatures(featuresCol)))
          .withColumn(s"model-6", bagging6(testLabAndFeatures(featuresCol)))

        // Majority wins
        val majorityVoter = udf {
          (tools: mutable.WrappedArray[Double]) => {
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
        evalMajority.printResult(s"majority vote on $datasetName for all tools with metadata")


      }
    }
  }

}
