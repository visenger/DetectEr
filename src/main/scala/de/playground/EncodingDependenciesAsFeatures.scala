package de.playground

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.HospSchema
import de.evaluation.f1.{Cells, Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.metadata.FD
import de.experiments.metadata.ToolsAndMetadataCombinerRunner.getDecisionTreeModels
import de.model.util.{FormatUtil, ModelUtil}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

class EncodingDependenciesAsFeatures {

}



object EncodingDependenciesAsFeaturesPlayground extends ExperimentsCommonConfig {
  val datasetName = "hosp"

  val dirtyData = allRawData.getOrElse(datasetName, "unknown")
  val mainSchema = allSchemasByName.getOrElse(datasetName, HospSchema)
  val schema = mainSchema.getSchema

  val metadataPath = allMetadataByName.getOrElse(datasetName, "unknown")
  val trainDataPath = allTrainData.getOrElse(datasetName, "unknown")
  val testDataPath = allTestData.getOrElse(datasetName, "unknown")

  val featuresCol = "features"

  val config: Config = ConfigFactory.load()

  def main(args: Array[String]): Unit = {


    SparkLOAN.withSparkSession("FD-ENCODING") {
      session => {
        import org.apache.spark.sql.functions._

        val dirtyDF = DataSetCreator
          .createFrame(session, dirtyData, schema: _*).cache()

        //step 2: content-based medatadata
        val contentMetadataDF: DataFrame = plgrd_extractContentBasedMetadata(session, dirtyDF)
        /**
          *
          * +-----+------+--------------------+-----------------+--
          * |RecID|attrNr|               value|metadata         /
          * +-----+------+--------------------+-----------------+--
          *
          **/

        //step 3:  INTER-COLUMN DEPENDENCIES

        val zip = "zip"
        val city = "city"
        val phone = "phone"
        val address = "address"
        val state = "state"
        val prno = "prno"
        val mc = "mc"
        val stateavg = "stateavg"

        /** ALL FDs for the hosp data
          * zip -> city
          * zip -> state
          * zip, address -> phone
          * city, address -> phone
          * state, address -> phone
          * prno, mc -> stateavg
          * */

        val fd1 = FD(List(zip), List(city, state))
        val fd2 = FD(List(zip, address), List(phone))
        val fd3 = FD(List(city, address), List(phone))
        val fd4 = FD(List(state, address), List(phone))
        val fd5 = FD(List(prno, mc), List(stateavg))

        val allFDs: List[FD] = List(fd1, fd2, fd3, fd4, fd5)

        /*val allPossibleFDCombis: immutable.Seq[List[FD]] =
          (2 until allFDs.size)
            .flatMap(size => allFDs.combinations(size)) */
        val allPossibleFDCombis: Seq[List[FD]] = allFDs.combinations(2).toSeq

        allPossibleFDCombis.foreach(datasetFDs => {

          println(
            s"""
               |
               |==========================================================="""
              .stripMargin)
          println(s"processing fd combinations: ${datasetFDs.mkString("[", "; ", "]")}")

          val fdsDataframe: DataFrame = plgrd_extractFDMetadata(session, dirtyDF, datasetFDs)
          /**
            *
            * +-----+------+--------------------+-----------------+--
            * |RecID|attrNr|               value|fds               /
            * +-----+------+--------------------+-----------------+--
            *
            **/

          val fullMetadataDF = fdsDataframe.join(contentMetadataDF, Seq(Cells.recid, Cells.attrnr, "value"))

          val allMetadataCols = Array("fds", "metadata")
          val fullVecAssembler = new VectorAssembler()
            .setInputCols(allMetadataCols)
            .setOutputCol("full-metadata")
          val metadataDF = fullVecAssembler.transform(fullMetadataDF).drop(allMetadataCols: _*)

          //step 1: system features generation

          val trainDF = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*).cache()
          val testDF = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*).cache()


          val trainToolsAndMetadataDF = trainDF.join(metadataDF, Seq(FullResult.recid, FullResult.attrnr))

          val testToolsAndMetadataDF = testDF.join(metadataDF, Seq(FullResult.recid, FullResult.attrnr))

          val transformToToolsVector = udf {
            (tools: mutable.WrappedArray[String]) => {
              val values = tools.map(t => t.toDouble).toArray
              Vectors.dense(values)
            }
          }

          val tools = FullResult.tools
          val trainToolsCols = tools.map(t => trainDF(t)).toArray
          val testToolsCols = tools.map(t => testDF(t)).toArray

          val trainToolsVectorDF = trainToolsAndMetadataDF
            .withColumn("tools-vector", transformToToolsVector(array(trainToolsCols: _*)))
          val testToolsVectorDF = testToolsAndMetadataDF
            .withColumn("tools-vector", transformToToolsVector(array(testToolsCols: _*)))

          val toolsAndMetadataAssembler = new VectorAssembler()
            .setInputCols(Array("tools-vector", "full-metadata"))
            .setOutputCol("features")

          val colNames = List("RecID", "attrNr", "value", "full-metadata", "tools-vector")

          val trainFullFeaturesDF = toolsAndMetadataAssembler
            .transform(trainToolsVectorDF)
            .drop(colNames: _*)
            .drop(FullResult.tools: _*)
          val testFullFeaturesDF = toolsAndMetadataAssembler
            .transform(testToolsVectorDF)
            .drop(colNames: _*)
            .drop(FullResult.tools: _*)
          //trainFullFeaturesDF.show(68)


          //          val evalBagging = plgrd_performBaggingOnToolsAndMetadata(session, trainFullFeaturesDF, testFullFeaturesDF)
          //          evalBagging.printResult(s"BAGGING: FULL METADATA (FDs and content-based) AND TOOLS RESULT FOR $datasetName")


          val evalStacking = plgrd_performStackingOnToolsAndMetadata(session, trainFullFeaturesDF, testFullFeaturesDF)
          evalStacking.printResult(s"STACKING: FULL METADATA (FDs and content-based) AND TOOLS RESULT FOR $datasetName")

        })


      }
    }
  }

  private def generateFDName(fd: FD) = {
    fd.getFD.mkString("").hashCode
  }

  def plgrd_extractFDMetadata(session: SparkSession, dirtyDF: DataFrame, datasetFDs: List[FD]): DataFrame = {
    import org.apache.spark.sql.functions._

    val allFDEncodings: List[DataFrame] = datasetFDs.map(fd => {

      val fd0 = fd.getFD
      val lhs = fd.lhs
      val lhsFD: List[Column] = lhs.map(dirtyDF(_))

      //grouping is done by the LHS of the fd.
      val lhsCounts: DataFrame = dirtyDF
        .groupBy(lhsFD: _*)
        .count()

      val clustersForFD: DataFrame = lhsCounts
        .where(lhsCounts("count") > 1)
        .withColumn("cluster-id", concat_ws("-", lit("clust"), monotonically_increasing_id() + 1))
        .toDF()

      //clustersForFD.printSchema()
      println(s"FD processed: ${fd.toString}")
      println(s"number of clusters for the FD : ${clustersForFD.count()}")

      /*
    *  @param joinType Type of join to perform. Default `inner`. Must be one of:
   *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   *                 `right`, `right_outer`, `left_semi`, `left_anti`.
    * */

      //clust-zero is a placeholder for any attributes, which do not have pairs.(but belong to some fd)
      val defaultValForCells = "clust-zero"
      val joinedWithGroups: DataFrame = dirtyDF
        .join(clustersForFD, lhs, "left_outer")
        .na.fill(0, Seq("count"))
        .na.fill(defaultValForCells, Seq("cluster-id"))

      // joinedWithGroups.show()

      val attributes: Seq[String] = HospSchema.getSchema.filterNot(_.equals(HospSchema.getRecID))

      val cluster_fd = udf {
        (value: String, attrName: String, fd: mutable.WrappedArray[String]) => {
          //all values, not members of any fd will get a default value "no-fd"
          val attrIsNotInFD = "no-fd"
          val valueForFD: String = if (fd.contains(attrName)) value else attrIsNotInFD

          valueForFD
        }
      }
      val attrDFs: Seq[DataFrame] = attributes.map(attr => {
        val attrIdx = HospSchema.getIndexesByAttrNames(List(attr)).head
        val attrDF = joinedWithGroups.select(HospSchema.getRecID, attr, "cluster-id")
          .withColumn(FullResult.attrnr, lit(attrIdx))
          .withColumn("clusters-fd", cluster_fd(joinedWithGroups("cluster-id"), lit(attr), array(fd0.map(lit(_)): _*)))
          .toDF(FullResult.recid, "value", "cluster-id", FullResult.attrnr, "clusters-fd")

        attrDF
          .select(FullResult.recid, FullResult.attrnr, "value", "clusters-fd")
          .toDF()
      })

      val fdIdx = generateFDName(fd)
      val fdsEncoded: DataFrame = attrDFs
        .reduce((df1, df2) => df1.union(df2))
        .repartition(1)
        .toDF(FullResult.recid, FullResult.attrnr, "value", s"fd-${fdIdx}")


      val fdIndexer = new StringIndexer()
        .setInputCol(s"fd-${fdIdx}")
        .setOutputCol(s"fd-${fdIdx}-idx")
      val fdIndexedDF = fdIndexer.fit(fdsEncoded).transform(fdsEncoded).drop(s"fd-${fdIdx}")

      val oneHotEncoderForFD = new OneHotEncoder()
        .setDropLast(false)
        .setInputCol(s"fd-${fdIdx}-idx")
        .setOutputCol(s"fd-${fdIdx}-vec")
      val dfVectorizedDF = oneHotEncoderForFD.transform(fdIndexedDF).drop(s"fd-${fdIdx}-idx")

      dfVectorizedDF
    })

    val joinedFDs = allFDEncodings
      .reduce((fd1, fd2) => fd1.join(fd2, Seq(FullResult.recid, FullResult.attrnr, "value")))
    // joinedFDs.show()

    val fdArray: Array[String] = datasetFDs.map(fd => s"fd-${generateFDName(fd)}-vec").toArray

    val vectorAssembler = new VectorAssembler()
      .setInputCols(fdArray)
      .setOutputCol("fds") //all encodings for the functional dependencies

    val fdsDataframe = vectorAssembler.transform(joinedFDs).drop(fdArray: _*)
    fdsDataframe
  }

  def plgrd_extractContentBasedMetadata(session: SparkSession, dirtyDF: DataFrame): DataFrame = {
    //TODO: content-based metadata
    import org.apache.spark.sql.functions._
    val getTypeByAttrName = udf {
      attr: String => {
        mainSchema.dataTypesDictionary.getOrElse(attr, "unknown")
      }
    }

    val attributesDFs: Seq[DataFrame] = schema
      .filterNot(_.equalsIgnoreCase(mainSchema.getRecID))
      .map(attribute => {
        val indexByAttrName = mainSchema.getIndexesByAttrNames(List(attribute)).head
        val flattenDF = dirtyDF
          .select(mainSchema.getRecID, attribute)
          .withColumn("attrName", lit(attribute))
          .withColumn("attr", lit(indexByAttrName))
          .withColumn("attrType", getTypeByAttrName(lit(attribute)))
          .withColumn("isNull", isnull(dirtyDF(attribute)))
          .withColumn("value", dirtyDF(attribute))

        flattenDF
          .select(mainSchema.getRecID, "attrName", "attr", "attrType", "isNull", "value")
      })

    val unionAttributesDF: DataFrame = attributesDFs
      .reduce((df1, df2) => df1.union(df2))
      .repartition(1)
      .toDF(Cells.recid, "attrName", Cells.attrnr, "attrType", "isNull", "value")

    val isMissingValue = udf { value: Boolean => {
      if (value) 1.0 else 0.0
    }
    }
    val metaDF = unionAttributesDF
      .withColumn("missingValue", isMissingValue(unionAttributesDF("isNull")))

    val typeIndexer = new StringIndexer()
      .setInputCol("attrType")
      .setOutputCol("attrTypeIndex")
    val indexedTypesDF = typeIndexer.fit(metaDF).transform(metaDF)

    val typeEncoder = new OneHotEncoder()
      .setDropLast(false)
      .setInputCol("attrTypeIndex")
      .setOutputCol("attrTypeVector")
    val dataTypesEncodedDF = typeEncoder.transform(indexedTypesDF)

    //dataTypesEncodedDF.printSchema()

    val top10Values = new MetadataCreator()
      .extractTop10Values(session, metadataPath)
      .cache()
      .toDF("attrNameMeta", "top10")

    val top10List: List[String] = top10Values
      .select("top10")
      .rdd
      .map(row => row.getAs[String](0))
      .collect()
      .toList

    val isTop10Values = udf {
      (value: String, attrName: String) => {
        top10List.contains(value) match {
          case true => 1.0
          case false => 0.0
        }
      }
    }

    val withTop10MetadataDF = dataTypesEncodedDF
      .withColumn("isTop10", isTop10Values(dataTypesEncodedDF("value"), dataTypesEncodedDF("attrName")))

    //final assember of all content-based metadata.
    val assembler = new VectorAssembler()
      .setInputCols(Array("missingValue", "attrTypeVector", "isTop10"))
      .setOutputCol("metadata")

    val contentBasedMetadataCols: List[String] = List("attrName", "attrType", "isNull", "missingValue", "attrTypeIndex", "attrTypeVector", "isTop10")
    val contentMetadataDF = assembler.transform(withTop10MetadataDF).drop(contentBasedMetadataCols: _*)
    //contentMetadataDF.show(false)
    contentMetadataDF
  }

  def plgrd_performStackingOnToolsAndMetadata(session: SparkSession, trainFull: DataFrame, test: DataFrame): Eval = {
    import session.implicits._

    //todo: setting training data to 1%
    val Array(train, _) = trainFull.randomSplit(Array(0.1, 0.9))

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

    val nnPredictionAndLabel = FormatUtil.getPredictionAndLabel(nnPrediction, "nn-prediction")
    val nnEval = F1.evalPredictionAndLabels(nnPredictionAndLabel)
    nnEval.printResult(s"neural networks on $datasetName with metadata and FDs")
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

    val dtPredictionAndLabel = FormatUtil.getPredictionAndLabel(dtTrainPrediction, "dt-prediction")
    val dtEval = F1.evalPredictionAndLabels(dtPredictionAndLabel)
    dtEval.printResult(s"decision tree on $datasetName with metadata and FDs")
    //end: decision tree

    //start: bayes
    val bayesModel = NaiveBayes.train(trainLabPointRDD, lambda = 1.0, modelType = "bernoulli")
    val predictByBayes = udf { features: org.apache.spark.ml.linalg.Vector => {
      val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
      bayesModel.predict(transformedFeatures)
    }
    }

    val nbPrediction = dtPrediction.withColumn("nb-prediction", predictByBayes(dtPrediction(featuresCol)))
    val nbTrainPrediction = dtTrainPrediction.withColumn("nb-prediction", predictByBayes(dtTrainPrediction(featuresCol)))

    val nbPredictionAndLabel = FormatUtil.getPredictionAndLabel(nbTrainPrediction, "nb-prediction")
    val nbEval = F1.evalPredictionAndLabels(nbPredictionAndLabel)
    nbEval.printResult(s"naive bayes on $datasetName with metadata and FDs")

    // meta classifier: logreg

    val assembler = new VectorAssembler()
      .setInputCols(Array("nn-prediction", "dt-prediction", "nb-prediction"))
      .setOutputCol("all-predictions")

    //Meta train
    val allTrainPrediction = assembler.transform(nbTrainPrediction).select(FullResult.label, "all-predictions")
    val allTrainClassifiers = allTrainPrediction.withColumnRenamed("all-predictions", featuresCol)

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

  def plgrd_performBaggingOnToolsAndMetadata(session: SparkSession, trainFull: DataFrame, test: DataFrame): Eval = {
    import session.implicits._

    //todo: setting training data to 1%
    val Array(train, _) = trainFull.randomSplit(Array(0.1, 0.9))

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

}


