package de.experiments.metadata

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{SparkLOAN, Timer}
import de.experiments.models.combinator.{Bagging, Stacking}
import de.model.util.FormatUtil
import de.wrangling.WranglingDatasetsToMetadata
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

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

  private def getFeaturesNumber(featuresDF: DataFrame): Int = {
    featuresDF.select("features").head().getAs[org.apache.spark.ml.linalg.Vector](0).size
  }


}
