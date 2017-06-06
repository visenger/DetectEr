package de.experiments.models.combinator

import de.evaluation.f1.{Eval, F1, FullResult}
import de.model.util.FormatUtil
import de.wrangling.WranglingDatasetsToMetadata
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{array, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Bagging strategy for ensamble learning
  */
class Bagging {

  private var testDF: DataFrame = null
  private var trainDF: DataFrame = null
  private var dataset: String = ""

  private var allColumns: Seq[String] = FullResult.tools

  def onDataSetName(name: String): this.type = {
    dataset = name
    this
  }

  def onTrainDataFrame(df: DataFrame): this.type = {
    trainDF = df
    this
  }

  def onTestDataFrame(df: DataFrame): this.type = {
    testDF = df
    this
  }

  def useTools(tools: Seq[String]): this.type = {
    allColumns = tools
    this
  }

  def performEnsambleLearningOnTools(session: SparkSession): Eval = {

    val train = FormatUtil.prepareDataToLabeledPoints(session, trainDF, allColumns)

    val Array(train1, _) = train.randomSplit(Array(0.9, 0.1), seed = 123L)
    val Array(train2, _) = train.randomSplit(Array(0.9, 0.1), seed = 23L)
    val Array(train3, _) = train.randomSplit(Array(0.9, 0.1), seed = 593L)
    val Array(train4, _) = train.randomSplit(Array(0.9, 0.1), seed = 941L)
    val Array(train5, _) = train.randomSplit(Array(0.9, 0.1), seed = 3L)
    val Array(train6, _) = train.randomSplit(Array(0.9, 0.1), seed = 623L)

    val trainSamples = Seq(train1, train2, train3, train4, train5, train6)

    val Array(model1, model2, model3, model4, model5, model6) = getDecisionTreeModelsForTools(trainSamples, allColumns).toArray

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
    // evalMajority.printResult(s"BAGGING (DT): $dataset majority vote for ${allColumns.mkString(", ")}")

    evalMajority
  }

  def performEnsambleLearningOnToolsAndMetadata(session: SparkSession): Eval = {
    import session.implicits._

    val (trainFull, test) = new WranglingDatasetsToMetadata()
      .onDatasetName(dataset)
      .onTools(allColumns)
      .createMetadataFeatures(session)
    //todo: setting training data to 1%
    val Array(train, _) = trainFull.randomSplit(Array(0.1, 0.9))

    val featuresCol = "features"

    val trainLabPointRDD: RDD[LabeledPoint] = FormatUtil
      .prepareDFToLabeledPointRDD(session, train)

    val testLabAndFeatures: DataFrame = FormatUtil
      .prepareDFToLabeledPointRDD(session, test)
      .toDF(FullResult.label, featuresCol)

    //APPLY CLASSIFICATION
    //start: decision tree

    val Array(_, train1) = trainLabPointRDD.randomSplit(Array(0.3, 0.7), seed = 123L)
    val Array(train2, _) = trainLabPointRDD.randomSplit(Array(0.7, 0.3), seed = 23L)
    val Array(_, train3) = trainLabPointRDD.randomSplit(Array(0.3, 0.7), seed = 593L)
    val Array(train4, _) = trainLabPointRDD.randomSplit(Array(0.7, 0.3), seed = 941L)
    val Array(_, train5) = trainLabPointRDD.randomSplit(Array(0.3, 0.7), seed = 3L)
    val Array(train6, _) = trainLabPointRDD.randomSplit(Array(0.7, 0.3), seed = 623L)

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
    //evalMajority.printResult(s"majority vote on $dataset for all tools with metadata")

    evalMajority
  }

  private def getFeaturesNumber(featuresDF: DataFrame): Int = {
    featuresDF.select("features").head().getAs[org.apache.spark.ml.linalg.Vector](0).size
  }

  private def getDecisionTreeModels(trainSamples: Seq[RDD[LabeledPoint]], toolsNum: Int): Seq[DecisionTreeModel] = {
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

  private def getDecisionTreeModelsForTools(trainSamples: Seq[RDD[LabeledPoint]], tools: Seq[String]): Seq[DecisionTreeModel] = {
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

}
