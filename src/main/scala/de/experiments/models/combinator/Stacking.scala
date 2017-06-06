package de.experiments.models.combinator

import de.evaluation.f1.{Eval, F1, FullResult}
import de.experiments.ExperimentsCommonConfig
import de.experiments.metadata.ToolsAndMetadataCombinerRunner.getDecisionTreeModels
import de.model.logistic.regression.ModelData
import de.model.util.{FormatUtil, ModelUtil}
import de.wrangling.WranglingDatasetsToMetadata
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Perform ensamble learning by using stacking strategy.
  */
class Stacking extends ExperimentsCommonConfig {

  private var testDF: DataFrame = null
  private var trainDF: DataFrame = null
  private var dataset: String = ""

  private var allColumns: Seq[String] = FullResult.tools

  //  private val unionall = "unionAll"
  //  private val minKCol = "minK"
  //val maxK = "maxK"
  //  val majorVote = "major"
  private val naiveBayesCol = "naive-bayes"
  private val dtCol = "decision-tree"
  private val logRegrCol = "log-regr"
  private val predictCol = "prediction"
  private val nNetworksCol = "n-networks"

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
    import session.implicits._

    val maxPrecision = experimentsConf.getDouble(s"${dataset}.max.precision")
    val maxRecall = experimentsConf.getDouble(s"${dataset}.max.recall")

    //val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
    //val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

    val allTools = allColumns
    val trainLabeledPointsTools: RDD[LabeledPoint] = FormatUtil
      .prepareDataToLabeledPoints(session, trainDF, allTools)

    val bayesModel = NaiveBayes.train(trainLabeledPointsTools, lambda = 1.0, modelType = "bernoulli")
    val (bestLogRegrData, bestLogRegModel) = ModelUtil.getBestModel(maxPrecision, maxRecall, trainLabeledPointsTools, trainLabeledPointsTools)

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

    //    val predictByLogRegr = udf { features: org.apache.spark.mllib.linalg.Vector => {
    //      bestLogRegModel.setThreshold(bestLogRegrData.bestThreshold)
    //      bestLogRegModel.predict(features)
    //    }
    //    }
    val predictByBayes = udf { features: org.apache.spark.mllib.linalg.Vector => bayesModel.predict(features) }

    //    val minKTools = udf { (k: Int, tools: org.apache.spark.mllib.linalg.Vector) => {
    //      val sum = tools.numNonzeros
    //      val errorDecision = if (sum >= k) 1.0 else 0.0
    //      errorDecision
    //    }
    //    }

    val features = "features"
    val rowId = "row-id"

    val testLabeledPointsTools: DataFrame = FormatUtil
      .prepareDataToLabeledPoints(session, testDF, allColumns)
      .toDF(FullResult.label, features)
      .withColumn(rowId, monotonically_increasing_id())


    var allClassifiers = testLabeledPointsTools
      .withColumn(dtCol, predictByDT(testLabeledPointsTools(features)))
      .withColumn(naiveBayesCol, predictByBayes(testLabeledPointsTools(features)))
      //.withColumn(logRegrCol, predictByLogRegr(testLabeledPointsTools(features)))
      // .withColumn(unionall, minKTools(lit(1), testLabeledPointsTools(features)))
      //  .withColumn(minKCol, minKTools(lit(toolsNum), testLabeledPointsTools(features)))
      .select(rowId, FullResult.label, dtCol, naiveBayesCol)
      .toDF()

    //start:neural networks
    val nextLayer = toolsNum + 1
    val layers = Array[Int](toolsNum, nextLayer, toolsNum, numClasses)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
    val nnTrain: DataFrame = FormatUtil.prepareDataToLIBSVM(session, trainDF, allColumns)
    val networkModel = trainer.fit(nnTrain)
    val testWithRowId = testDF.withColumn(rowId, monotonically_increasing_id())
    val nnTest = FormatUtil.prepareDataWithRowIdToLIBSVM(session, testWithRowId, allColumns)
    val result = networkModel.transform(nnTest)
    val nnPrediction = result.select(rowId, predictCol)
    //end:neural networks


    allClassifiers = allClassifiers
      .join(nnPrediction, rowId)
      .withColumnRenamed(predictCol, nNetworksCol)
      .select(rowId, FullResult.label, nNetworksCol, dtCol, naiveBayesCol)


    //all possible combinations of errors classification and their counts
    //    val allResults = allClassifiers
    //      .rdd
    //      .map(row => {
    //        //val rowId = row.getLong(0)
    //        val label = row.getDouble(1)
    //        val nn = row.getDouble(2)
    //        val dt = row.getDouble(3)
    //        val bayes = row.getDouble(4)
    //        val logregr = row.getDouble(5)
    //        val unionAll = row.getDouble(6)
    //        val minK = row.getDouble(7)
    //        (label, nn, dt, bayes, logregr, unionAll, minK)
    //      })
    //
    //    val countByValue = allResults.countByValue()
    //
    //    println(s"label, nn, dt, nb, logreg, unionall, minK: count")
    //    countByValue.foreach(entry => {
    //      println(s"(${entry._1._1.toInt}, ${entry._1._2.toInt}, ${entry._1._3.toInt}, ${entry._1._4.toInt}, ${entry._1._5.toInt}, ${entry._1._6.toInt}, ${entry._1._7.toInt}) : ${entry._2}")
    //    })

    //F1 for all classifiers performed on tools result:
    //    val predAndLabelNN = FormatUtil.getPredictionAndLabel(allClassifiers, nNetworksCol)
    //    val evalNN = F1.evalPredictionAndLabels(predAndLabelNN)
    //    evalNN.printResult("Neural Networks")

    //    val predictionAndLabelDT = FormatUtil.getPredictionAndLabel(allClassifiers, dtCol)
    //    val evalDTrees = F1.evalPredictionAndLabels(predictionAndLabelDT)
    //    evalDTrees.printResult("decision trees")

    //    val predAndLabelLogReg = FormatUtil.getPredictionAndLabel(allClassifiers, logRegrCol)
    //    val evalLogReg = F1.evalPredictionAndLabels(predAndLabelLogReg)
    //    evalLogReg.printResult("logistic regression")

    //    val predictionAndLabelNB = FormatUtil.getPredictionAndLabel(allClassifiers, naiveBayesCol)
    //    val evalNB = F1.evalPredictionAndLabels(predictionAndLabelNB)
    //    evalNB.printResult("naive bayes")


    //Now combine all classifiers:
    //    println(s"COMBINE CLASSIFIERS")

    //    val minK = udf { (k: Int, tools: mutable.WrappedArray[Double]) => {
    //      val sum = tools.count(_ == 1.0)
    //      val errorDecision = if (sum >= k) 1.0 else 0.0
    //      errorDecision
    //    }
    //    }

    //    val clColumns: Array[Column] = Array(
    //      allClassifiers(nNetworksCol),
    //      allClassifiers(dtCol),
    //      allClassifiers(naiveBayesCol),
    //      allClassifiers(logRegrCol),
    //      allClassifiers(unionall),
    //      allClassifiers(minKCol))

    //    val finalCombiCol = "final-combi"

    //    (1 to clColumns.length).foreach(k => {
    //      val finalResult: DataFrame =
    //        allClassifiers
    //          .withColumn(finalCombiCol, minK(lit(k), array(clColumns: _*)))
    //
    //      val predictionAndLabel = FormatUtil.getPredictionAndLabel(finalResult, finalCombiCol)
    //      val eval = F1.evalPredictionAndLabels(predictionAndLabel)
    //      eval.printResult(s"min-$k combination of nn, dt, nb, logreg, unionall, min5")
    //    })

    //    val maxKClassifiers = udf { (k: Int, tools: mutable.WrappedArray[Double]) => {
    //      val sum = tools.count(_ == 1.0)
    //      val errorDecision = if (1 to k contains sum) 1.0 else 0.0
    //      errorDecision
    //    }
    //    }

    // Majority wins
    //    val majorityVoter = udf { (tools: mutable.WrappedArray[Double]) => {
    //      val total = tools.length
    //      val sum1 = tools.count(_ == 1.0)
    //      var sum0 = total - sum1
    //      val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
    //      errorDecision
    //    }
    //    }
    //
    //    val majorityVoterCol = "majority-voter"
    //    val majorityVoterStrategy = allClassifiers
    //      .withColumn(majorityVoterCol, majorityVoter(array(clColumns: _*)))
    //      .select(FullResult.label, majorityVoterCol)
    //      .toDF()
    //
    //    val majorityVotersPredAndLabels = FormatUtil.getPredictionAndLabel(majorityVoterStrategy, majorityVoterCol)
    //    val majorityEval = F1.evalPredictionAndLabels(majorityVotersPredAndLabels)
    //    majorityEval.printResult(": majority voters")
    //end: Majority wins


    val allClassifiersCols: Seq[String] = Array(nNetworksCol, dtCol, naiveBayesCol).toSeq

    val labeledPointsClassifiers = FormatUtil.prepareDoublesToLabeledPoints(session, allClassifiers, allClassifiersCols)

    val Array(trainClassifiers, testClassifiers) = labeledPointsClassifiers.randomSplit(Array(0.2, 0.8))

    //Logistic Regression for classifier combination:
    val (classiBestModelData, classiBestModel) =
    //todo: Achtung: we removed the max precision and max recall threshold
      ModelUtil.getBestModel(trainClassifiers, trainClassifiers)

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
    //evalCombi.printResult("linear combi of all")

    evalCombi


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

    val featuresNumber = getFeaturesNumber(train)
    val numClasses = 2

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
    //    val nnPredictionAndLabel = FormatUtil.getPredictionAndLabel(nnPrediction, "nn-prediction")
    //    val nnEval = F1.evalPredictionAndLabels(nnPredictionAndLabel)
    //    nnEval.printResult(s"neural networks on $dataset with metadata")
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
    //    val dtPredictionAndLabel = FormatUtil.getPredictionAndLabel(dtPrediction, "dt-prediction")
    //    val dtEval = F1.evalPredictionAndLabels(dtPredictionAndLabel)
    //    dtEval.printResult(s"decision tree on $dataset with metadata")
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
    //    val nbPredictionAndLabel = FormatUtil.getPredictionAndLabel(nbPrediction, "nb-prediction")
    //    val nbEval = F1.evalPredictionAndLabels(nbPredictionAndLabel)
    //    nbEval.printResult(s"naive bayes on $dataset with metadata")
    //end: bayes


    // meta classifier: logreg

    val assembler = new VectorAssembler()
      .setInputCols(Array("nn-prediction", "dt-prediction", "nb-prediction"))
      .setOutputCol("all-predictions")

    //Meta train
    val allTrainPrediction = assembler.transform(nbTrainPrediction).select(FullResult.label, "all-predictions")
    val allTrainClassifiers = allTrainPrediction.withColumnRenamed("all-predictions", featuresCol)

    //Train LogModel
    val trainLabeledRDD: RDD[LabeledPoint] = FormatUtil.prepareDFToLabeledPointRDD(session, allTrainClassifiers)
    val (modelData, lrModel) = ModelUtil.getBestModel(trainLabeledRDD, trainLabeledRDD)

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
    //lrEval.printResult(s"STACKING RESULT FOR $dataset")
    lrEval
  }

  private def getFeaturesNumber(featuresDF: DataFrame): Int = {
    featuresDF.select("features").head().getAs[org.apache.spark.ml.linalg.Vector](0).size
  }

  //Todo: remove thresholds from learning model.
  private def _getBestModel(maxPrecision: Double,
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
