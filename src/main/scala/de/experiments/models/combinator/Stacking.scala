package de.experiments.models.combinator

import de.evaluation.f1.{Eval, F1, FullResult}
import de.experiments.ExperimentsCommonConfig
import de.model.util.{Features, FormatUtil, ModelUtil}
import de.wrangling.WranglingDatasetsToMetadata
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.NaiveBayes
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
  private val featuresCol = "features"

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

  private val allPredictionsCol = "all-predictions"

  def old_performStackingOnToolsAndMetadata(session: SparkSession, trainFull: DataFrame, test: DataFrame): Eval = {
    import session.implicits._

    //todo: setting training data to 1%
    val Array(train, _) = trainFull.randomSplit(Array(0.1, 0.9))

    val trainLabPointRDD: RDD[LabeledPoint] = FormatUtil
      .prepareDFToLabeledPointRDD(session, train)

    //println(s"initial features vector size= ${trainLabPointRDD.first().features.size}")


    val testLabAndFeatures: DataFrame = FormatUtil
      .prepareDFToLabeledPointRDD(session, test)
      .toDF(FullResult.label, featuresCol)

    val featuresNumber = getFeaturesNumber(train)
    // val numClasses = 2

    //start:neural networks
    //    val nextLayer = featuresNumber + 1
    //    val layers = Array[Int](featuresNumber, nextLayer, featuresNumber, numClasses)
    //    val trainer = new MultilayerPerceptronClassifier()
    //      .setLayers(layers)
    //      .setBlockSize(128)
    //      .setSeed(1234L)
    //      .setMaxIter(100)
    //    val trainDF = trainLabPointRDD.toDF(FullResult.label, featuresCol)
    //    val nnTrain: DataFrame = FormatUtil.convertVectors(session, trainDF, featuresCol)
    //    val networkModel = trainer.fit(nnTrain)
    //    val nnTest: DataFrame = FormatUtil.convertVectors(session, testLabAndFeatures, featuresCol)
    //    val result: DataFrame = networkModel.transform(nnTest)
    //    val resultTrain: DataFrame = networkModel.transform(nnTrain)
    //    val nnTrainPrediction = resultTrain.withColumnRenamed("prediction", "nn-prediction")
    //    val nnPrediction: DataFrame = result.withColumnRenamed("prediction", "nn-prediction")
    //
    //    val nnPredictionAndLabel = FormatUtil.getPredictionAndLabel(nnPrediction, "nn-prediction")
    //    val nnEval = F1.evalPredictionAndLabels(nnPredictionAndLabel)
    //    nnEval.printResult(s"neural networks on $dataset with metadata and FDs")
    //end:neural networks


    //start: decision tree:
    val decisionTreeModel: DecisionTreeModel = ModelUtil.getDecisionTreeModels(Seq(trainLabPointRDD), featuresNumber).head

    val predictByDT = udf { features: org.apache.spark.ml.linalg.Vector => {
      val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
      decisionTreeModel.predict(transformedFeatures)
    }
    }

    val trainLabPointDF = trainLabPointRDD.toDF(FullResult.label, featuresCol)

    val trainConvertedVecDF = FormatUtil.convertVectors(session, trainLabPointDF, featuresCol)
    val testConvertedVecDF = FormatUtil.convertVectors(session, testLabAndFeatures, featuresCol)

    val dtPrediction = trainConvertedVecDF
      .withColumn("dt-prediction", predictByDT(trainConvertedVecDF(featuresCol)))
    val dtTestPrediction = testConvertedVecDF
      .withColumn("dt-prediction", predictByDT(testConvertedVecDF(featuresCol)))

    //eval dt
    val dtPredictionAndLabel = FormatUtil.getPredictionAndLabel(dtTestPrediction, "dt-prediction")
    val dtEval = F1.evalPredictionAndLabels(dtPredictionAndLabel)
    dtEval.printResult(s"decision tree on $dataset with metadata and FDs")
    //end: decision tree

    //start: bayes
    //val bayesModel = NaiveBayes.train(trainLabPointRDD, lambda = 1.0, modelType = "bernoulli") //initial version
    val bayesModel = NaiveBayes.train(trainLabPointRDD, lambda = 1.0, modelType = "bernoulli")
    val predictByBayes = udf { features: org.apache.spark.ml.linalg.Vector => {
      val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)

      bayesModel.predict(transformedFeatures)
    }
    }


    // println(s"2. train feature vector size after dt ${dtPrediction.select(featuresCol).first().getAs[org.apache.spark.ml.linalg.Vector](featuresCol).size}")
    val nbPrediction = dtPrediction.withColumn("nb-prediction", predictByBayes(dtPrediction(featuresCol)))

    // println(s"3. test feature vector size after dt ${dtTestPrediction.select(featuresCol).first().getAs[org.apache.spark.ml.linalg.Vector](featuresCol).size}")
    val nbTestPrediction = dtTestPrediction.withColumn("nb-prediction", predictByBayes(dtTestPrediction(featuresCol)))

    //eval nb
    val nbPredictionAndLabel = FormatUtil.getPredictionAndLabel(nbTestPrediction, "nb-prediction")
    val nbEval = F1.evalPredictionAndLabels(nbPredictionAndLabel)
    nbEval.printResult(s"naive bayes on $dataset with metadata and FDs")

    // meta classifier: logreg

    val assembler = new VectorAssembler()
      .setInputCols(Array(/*"nn-prediction",*/ "dt-prediction", "nb-prediction"))
      .setOutputCol(allPredictionsCol)

    //Meta train
    val allTrainPrediction = assembler.transform(nbPrediction).select(FullResult.label, allPredictionsCol)
    val allTrainClassifiers = allTrainPrediction.withColumnRenamed(allPredictionsCol, featuresCol)

    //Train LogModel
    val trainLabeledRDD: RDD[LabeledPoint] = FormatUtil.prepareDFToLabeledPointRDD(session, allTrainClassifiers)


    //    val lrModel = new LogisticRegressionWithLBFGS()
    //      .setNumClasses(numClasses)
    //      .setIntercept(true)
    //      .run(trainLabeledRDD)

    val (modelData, lrModel) = ModelUtil.getBestLogRegressionModel(trainLabeledRDD, trainLabeledRDD)

    val logRegPredictor = udf { features: org.apache.spark.ml.linalg.Vector => {
      lrModel.setThreshold(modelData.bestThreshold)
      val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
      lrModel.predict(transformedFeatures)
    }
    }

    //Meta test
    val allPredictions = assembler.transform(nbTestPrediction).select(FullResult.label, allPredictionsCol)
    val allClassifiers = allPredictions.withColumnRenamed(allPredictionsCol, featuresCol)
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

  def runStackingOnToolsAndMetadata(session: SparkSession, trainFull: DataFrame, test: DataFrame): DataFrame = {
    import session.implicits._

    //todo: setting training data to 1%
    val Array(train, _) = trainFull.randomSplit(Array(0.1, 0.9))

    val trainLabPointRDD: RDD[LabeledPoint] = FormatUtil
      .prepareDFToLabeledPointRDD(session, train)


    val featuresNumber = getFeaturesNumber(train)
    // val numClasses = 2

    //start:neural networks
    //    val nextLayer = featuresNumber + 1
    //    val layers = Array[Int](featuresNumber, nextLayer, featuresNumber, numClasses)
    //    val trainer = new MultilayerPerceptronClassifier()
    //      .setLayers(layers)
    //      .setBlockSize(128)
    //      .setSeed(1234L)
    //      .setMaxIter(100)
    //    val trainDF = trainLabPointRDD.toDF(FullResult.label, featuresCol)
    //    val nnTrain: DataFrame = FormatUtil.convertVectors(session, trainDF, featuresCol)
    //    val networkModel = trainer.fit(nnTrain)
    //    val nnTest: DataFrame = FormatUtil.convertVectors(session, testLabAndFeatures, featuresCol)
    //    val result: DataFrame = networkModel.transform(nnTest)
    //    val resultTrain: DataFrame = networkModel.transform(nnTrain)
    //    val nnTrainPrediction = resultTrain.withColumnRenamed("prediction", "nn-prediction")
    //    val nnPrediction: DataFrame = result.withColumnRenamed("prediction", "nn-prediction")
    //
    //    val nnPredictionAndLabel = FormatUtil.getPredictionAndLabel(nnPrediction, "nn-prediction")
    //    val nnEval = F1.evalPredictionAndLabels(nnPredictionAndLabel)
    //    nnEval.printResult(s"neural networks on $dataset with metadata and FDs")
    //end:neural networks


    //start: decision tree:
    val decisionTreeModel: DecisionTreeModel = ModelUtil.getDecisionTreeModels(Seq(trainLabPointRDD), featuresNumber).head

    val predictByDT = udf { features: org.apache.spark.ml.linalg.Vector => {
      val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
      decisionTreeModel.predict(transformedFeatures)
    }
    }

    val predictTestByDT = udf { features: org.apache.spark.mllib.linalg.Vector => {
      decisionTreeModel.predict(features)
    }
    }

    val trainLabPointDF = trainLabPointRDD.toDF(FullResult.label, featuresCol)

    val trainConvertedVecDF = FormatUtil.convertVectors(session, trainLabPointDF, featuresCol)

    val testConvertedVecDF: DataFrame = FormatUtil
      .prepareTestDFToLabeledPointRDD(session, test)
      .toDF(FullResult.label, Features.featuresCol, FullResult.recid, FullResult.attrnr, FullResult.value)

    val dtPrediction = trainConvertedVecDF
      .withColumn("dt-prediction", predictByDT(trainConvertedVecDF(featuresCol)))
    val dtTestPrediction = testConvertedVecDF
      .withColumn("dt-prediction", predictTestByDT(testConvertedVecDF(featuresCol)))

    //eval dt
//    val dtPredictionAndLabel = FormatUtil.getPredictionAndLabel(dtTestPrediction, "dt-prediction")
//    val dtEval = F1.evalPredictionAndLabels(dtPredictionAndLabel)
//    dtEval.printResult(s"decision tree on $dataset with metadata and FDs")
    //end: decision tree

    //start: bayes
    val bayesModel = NaiveBayes.train(trainLabPointRDD, lambda = 1.0, modelType = "bernoulli")
    val predictByBayes = udf { features: org.apache.spark.ml.linalg.Vector => {
      val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
      bayesModel.predict(transformedFeatures)
    }
    }

    val predictTestByBayes = udf { features: org.apache.spark.mllib.linalg.Vector => {
      bayesModel.predict(features)
    }
    }

    // println(s"2. train feature vector size after dt ${dtPrediction.select(featuresCol).first().getAs[org.apache.spark.ml.linalg.Vector](featuresCol).size}")
    val nbPrediction = dtPrediction.withColumn("nb-prediction", predictByBayes(dtPrediction(featuresCol)))

    // println(s"3. test feature vector size after dt ${dtTestPrediction.select(featuresCol).first().getAs[org.apache.spark.ml.linalg.Vector](featuresCol).size}")
    val nbTestPrediction = dtTestPrediction.withColumn("nb-prediction", predictTestByBayes(dtTestPrediction(featuresCol)))

    //eval nb
//    val nbPredictionAndLabel = FormatUtil.getPredictionAndLabel(nbTestPrediction, "nb-prediction")
//    val nbEval = F1.evalPredictionAndLabels(nbPredictionAndLabel)
//    nbEval.printResult(s"naive bayes on $dataset with metadata and FDs")

    // meta classifier: logreg

    val assembler = new VectorAssembler()
      .setInputCols(Array(/*"nn-prediction",*/ "dt-prediction", "nb-prediction"))
      .setOutputCol(allPredictionsCol)

    //Meta train
    val allTrainPrediction = assembler
      .transform(nbPrediction)
      .select(FullResult.label, allPredictionsCol)

    val allTrainClassifiers = allTrainPrediction
      .withColumnRenamed(allPredictionsCol, featuresCol)

    //Train LogModel
    val trainLabeledRDD: RDD[LabeledPoint] = FormatUtil.prepareDFToLabeledPointRDD(session, allTrainClassifiers)

    val (modelData, lrModel) = ModelUtil.getBestLogRegressionModel(trainLabeledRDD, trainLabeledRDD)

    //Meta test
    var allPredictions = assembler
      .transform(nbTestPrediction)
      .withColumnRenamed(featuresCol, s"$featuresCol-initial") //we remove the old features, which are not relevant anymore for the meta-classifier

    val convert_vectors = udf {
      (initVec: org.apache.spark.ml.linalg.Vector) => org.apache.spark.mllib.linalg.Vectors.dense(initVec.toArray)
    }

    allPredictions = allPredictions
      .withColumn(featuresCol, convert_vectors(allPredictions(allPredictionsCol)))

    val logRegPredictor = udf { features: org.apache.spark.mllib.linalg.Vector => {
      lrModel.setThreshold(modelData.bestThreshold)
      lrModel.predict(features)
    }
    }
    val lrPredictorDF = allPredictions
      .withColumn("final-predictor", logRegPredictor(allPredictions(featuresCol)))
    lrPredictorDF
  }

  def performStackingOnToolsAndMetadata(session: SparkSession, trainFull: DataFrame, test: DataFrame): Eval = {
    /* import session.implicits._

     //todo: setting training data to 1%
     val Array(train, _) = trainFull.randomSplit(Array(0.1, 0.9))

     val trainLabPointRDD: RDD[LabeledPoint] = FormatUtil
       .prepareDFToLabeledPointRDD(session, train)


     val featuresNumber = getFeaturesNumber(train)
     // val numClasses = 2

     //start:neural networks
     //    val nextLayer = featuresNumber + 1
     //    val layers = Array[Int](featuresNumber, nextLayer, featuresNumber, numClasses)
     //    val trainer = new MultilayerPerceptronClassifier()
     //      .setLayers(layers)
     //      .setBlockSize(128)
     //      .setSeed(1234L)
     //      .setMaxIter(100)
     //    val trainDF = trainLabPointRDD.toDF(FullResult.label, featuresCol)
     //    val nnTrain: DataFrame = FormatUtil.convertVectors(session, trainDF, featuresCol)
     //    val networkModel = trainer.fit(nnTrain)
     //    val nnTest: DataFrame = FormatUtil.convertVectors(session, testLabAndFeatures, featuresCol)
     //    val result: DataFrame = networkModel.transform(nnTest)
     //    val resultTrain: DataFrame = networkModel.transform(nnTrain)
     //    val nnTrainPrediction = resultTrain.withColumnRenamed("prediction", "nn-prediction")
     //    val nnPrediction: DataFrame = result.withColumnRenamed("prediction", "nn-prediction")
     //
     //    val nnPredictionAndLabel = FormatUtil.getPredictionAndLabel(nnPrediction, "nn-prediction")
     //    val nnEval = F1.evalPredictionAndLabels(nnPredictionAndLabel)
     //    nnEval.printResult(s"neural networks on $dataset with metadata and FDs")
     //end:neural networks


     //start: decision tree:
     val decisionTreeModel: DecisionTreeModel = ModelUtil.getDecisionTreeModels(Seq(trainLabPointRDD), featuresNumber).head

     val predictByDT = udf { features: org.apache.spark.ml.linalg.Vector => {
       val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
       decisionTreeModel.predict(transformedFeatures)
     }
     }

     val predictTestByDT = udf { features: org.apache.spark.mllib.linalg.Vector => {
       decisionTreeModel.predict(features)
     }
     }

     val trainLabPointDF = trainLabPointRDD.toDF(FullResult.label, featuresCol)

     val trainConvertedVecDF = FormatUtil.convertVectors(session, trainLabPointDF, featuresCol)

     val testConvertedVecDF: DataFrame = FormatUtil
       .prepareTestDFToLabeledPointRDD(session, test)
       .toDF(FullResult.label, Features.featuresCol, FullResult.recid, FullResult.attrnr)

     val dtPrediction = trainConvertedVecDF
       .withColumn("dt-prediction", predictByDT(trainConvertedVecDF(featuresCol)))
     val dtTestPrediction = testConvertedVecDF
       .withColumn("dt-prediction", predictTestByDT(testConvertedVecDF(featuresCol)))

     //eval dt
     val dtPredictionAndLabel = FormatUtil.getPredictionAndLabel(dtTestPrediction, "dt-prediction")
     val dtEval = F1.evalPredictionAndLabels(dtPredictionAndLabel)
     dtEval.printResult(s"decision tree on $dataset with metadata and FDs")
     //end: decision tree

     //start: bayes
     val bayesModel = NaiveBayes.train(trainLabPointRDD, lambda = 1.0, modelType = "bernoulli")
     val predictByBayes = udf { features: org.apache.spark.ml.linalg.Vector => {
       val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
       bayesModel.predict(transformedFeatures)
     }
     }

     val predictTestByBayes = udf { features: org.apache.spark.mllib.linalg.Vector => {
       bayesModel.predict(features)
     }
     }

     // println(s"2. train feature vector size after dt ${dtPrediction.select(featuresCol).first().getAs[org.apache.spark.ml.linalg.Vector](featuresCol).size}")
     val nbPrediction = dtPrediction.withColumn("nb-prediction", predictByBayes(dtPrediction(featuresCol)))

     // println(s"3. test feature vector size after dt ${dtTestPrediction.select(featuresCol).first().getAs[org.apache.spark.ml.linalg.Vector](featuresCol).size}")
     val nbTestPrediction = dtTestPrediction.withColumn("nb-prediction", predictTestByBayes(dtTestPrediction(featuresCol)))

     //eval nb
     val nbPredictionAndLabel = FormatUtil.getPredictionAndLabel(nbTestPrediction, "nb-prediction")
     val nbEval = F1.evalPredictionAndLabels(nbPredictionAndLabel)
     nbEval.printResult(s"naive bayes on $dataset with metadata and FDs")

     // meta classifier: logreg

     val assembler = new VectorAssembler()
       .setInputCols(Array(/*"nn-prediction",*/ "dt-prediction", "nb-prediction"))
       .setOutputCol(allPredictionsCol)

     //Meta train
     val allTrainPrediction = assembler
       .transform(nbPrediction)
       .select(FullResult.label, allPredictionsCol)

     val allTrainClassifiers = allTrainPrediction
       .withColumnRenamed(allPredictionsCol, featuresCol)

     //Train LogModel
     val trainLabeledRDD: RDD[LabeledPoint] = FormatUtil.prepareDFToLabeledPointRDD(session, allTrainClassifiers)

     val (modelData, lrModel) = ModelUtil.getBestLogRegressionModel(trainLabeledRDD, trainLabeledRDD)

     //Meta test
     var allPredictions = assembler
       .transform(nbTestPrediction)
       .withColumnRenamed(featuresCol, s"$featuresCol-initial") //we remove the old features, which are not relevant anymore for the meta-classifier

     val convert_vectors = udf {
       (initVec: org.apache.spark.ml.linalg.Vector) => org.apache.spark.mllib.linalg.Vectors.dense(initVec.toArray)
     }

     allPredictions = allPredictions
       .withColumn(featuresCol, convert_vectors(allPredictions(allPredictionsCol)))

     val logRegPredictor = udf { features: org.apache.spark.mllib.linalg.Vector => {
       lrModel.setThreshold(modelData.bestThreshold)
       lrModel.predict(features)
     }
     }
     val lrPredictorDF = allPredictions
       .withColumn("final-predictor", logRegPredictor(allPredictions(featuresCol)))
 */

    val lrPredictorDF = runStackingOnToolsAndMetadata(session, trainFull, test)
    val lrPredictionAndLabel = FormatUtil.getPredictionAndLabel(lrPredictorDF, "final-predictor")
    val lrEval = F1.evalPredictionAndLabels(lrPredictionAndLabel)
    lrEval
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

    val allClassifiersCols: Seq[String] = Array(nNetworksCol, dtCol, naiveBayesCol).toSeq

    val labeledPointsClassifiers = FormatUtil.prepareDoublesToLabeledPoints(session, allClassifiers, allClassifiersCols)

    val Array(trainClassifiers, testClassifiers) = labeledPointsClassifiers.randomSplit(Array(0.2, 0.8))

    //Logistic Regression for classifier combination:
    val (classiBestModelData, classiBestModel) =
    //todo: Achtung: we removed the max precision and max recall threshold
      ModelUtil.getBestLogRegressionModel(trainClassifiers, trainClassifiers)

    val predictByLogRegrCombi = udf { features: org.apache.spark.mllib.linalg.Vector => {
      classiBestModel.setThreshold(classiBestModelData.bestThreshold)
      classiBestModel.predict(features)
    }
    }

    val testClassifiersDF = testClassifiers.toDF()

    val metaClassifierDF: DataFrame = testClassifiersDF
      .withColumn("lr-of-combi", predictByLogRegrCombi(testClassifiersDF(features)))


    val logRegrOfAllCombi: RDD[(Double, Double)] = metaClassifierDF
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
    val decisionTreeModel: DecisionTreeModel = ModelUtil.getDecisionTreeModels(Seq(trainLabPointRDD), featuresNumber).head

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
      .setOutputCol(allPredictionsCol)

    //Meta train
    val allTrainPrediction = assembler.transform(nbPrediction).select(FullResult.label, allPredictionsCol)
    val allTrainClassifiers = allTrainPrediction.withColumnRenamed(allPredictionsCol, featuresCol)

    //Train LogModel
    val trainLabeledRDD: RDD[LabeledPoint] = FormatUtil.prepareDFToLabeledPointRDD(session, allTrainClassifiers)
    val (modelData, lrModel) = ModelUtil.getBestLogRegressionModel(trainLabeledRDD, trainLabeledRDD)

    val logRegPredictor = udf { features: org.apache.spark.ml.linalg.Vector => {
      lrModel.setThreshold(modelData.bestThreshold)
      val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
      lrModel.predict(transformedFeatures)
    }
    }

    //Meta test
    val allPredictions = assembler.transform(nbTrainPrediction).select(FullResult.label, allPredictionsCol)
    val allClassifiers = allPredictions.withColumnRenamed(allPredictionsCol, featuresCol)
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


}
