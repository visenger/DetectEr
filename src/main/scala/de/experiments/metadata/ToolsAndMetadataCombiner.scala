package de.experiments.metadata

import de.evaluation.f1.{F1, FullResult}
import de.evaluation.util.{SparkLOAN, Timer}
import de.model.util.FormatUtil
import de.wrangling.WranglingDatasetsToMetadata
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
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
    Seq("blackoak", "hosp", "salaries").foreach(datasetName => {
      Timer.measureRuntime {
        () => runBaggingOnDecisionTreeFor(datasetName)
      }
    })

  }

  def runBaggingOnDecisionTreeFor(datasetName: String): Unit = {
    SparkLOAN.withSparkSession("METADATA") {
      session => {
        import session.implicits._

        val (train, test) = new WranglingDatasetsToMetadata()
          .onDatasetName(datasetName)
          .createMetadataFeatures(session)

        val featuresCol = "features"

        val trainLabPointRDD = FormatUtil
          .prepareDFToLabeledPointRDD(session, train)

        val testLabAndFeatures = FormatUtil
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

  private def getFeaturesNumber(featuresDF: DataFrame): Int = {
    featuresDF.select("features").head().getAs[org.apache.spark.ml.linalg.Vector](0).size
  }

}
