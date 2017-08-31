package de.playground

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.schema.HospSchema
import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.metadata.ToolsAndMetadataCombinerRunner.getDecisionTreeModels
import de.model.util.{FormatUtil, ModelUtil}
import de.wrangling.WranglingDatasetsToMetadata
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

class EncodingDependenciesAsFeatures {

}

case class FD(lhs: List[String], rhs: List[String]) {
  def getFD: List[String] = lhs ::: rhs
}

object EncodingDependenciesAsFeaturesPlayground {
  def main(args: Array[String]): Unit = {

    val config: Config = ConfigFactory.load()

    val dirtyData = "data.hosp.dirty.10k"


    /*

    *
    *  @param joinType Type of join to perform. Default `inner`. Must be one of:
   *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   *                 `right`, `right_outer`, `left_semi`, `left_anti`.
   *
    * */


    SparkLOAN.withSparkSession("FD-ENCODING") {
      session => {
        import org.apache.spark.sql.functions._

        val dirtyHospDF = DataSetCreator
          .createFrame(session, config.getString(dirtyData), HospSchema.getSchema: _*)


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

        val fd1 = FD(List(zip), List(city))
        val fd2 = FD(List(zip), List(state))
        val fd3 = FD(List(zip, address), List(phone))
        val fd4 = FD(List(city, address), List(phone))
        val fd5 = FD(List(state, address), List(phone))
        val fd6 = FD(List(prno, mc), List(stateavg))

        val datasetFDs: List[FD] = List(fd1, fd2, fd3, fd4, fd5, fd6)

        datasetFDs.foreach(fd => {

          val fd0 = fd.getFD
          val lhs = fd.lhs
          val lhsFD: List[Column] = lhs.map(dirtyHospDF(_))

          //grouping is done by the LHS of the fd.
          val lhsCounts: DataFrame = dirtyHospDF
            .groupBy(lhsFD: _*)
            .count()

          val clustersForFD: DataFrame = lhsCounts
            .where(lhsCounts("count") > 1)
            .withColumn("cluster-id", concat_ws("-", lit("clust"), monotonically_increasing_id() + 1))
            .toDF()

          clustersForFD.printSchema()


          println(s"number of clusters for the FD1: ${clustersForFD.count()}")

          //clust-zero is a placeholder for any attributes, which do not have pairs.(but belong to some fd)
          val defaultValForCells = "clust-zero"
          val joinedWithGroups: DataFrame = dirtyHospDF
            .join(clustersForFD, lhs, "left_outer")
            .na.fill(0, Seq("count"))
            .na.fill(defaultValForCells, Seq("cluster-id"))

          joinedWithGroups.show()

          val attributes: Seq[String] = HospSchema.getSchema.filterNot(_.equals(HospSchema.getRecID))

          println(s" all attributes: ${attributes.mkString(", ")}")

          //        joinedWithGroups.where("count = 0").show()

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

          val fdsEncoded: DataFrame = attrDFs
            .reduce((df1, df2) => df1.union(df2))
            .repartition(1)
            .toDF(FullResult.recid, FullResult.attrnr, "value", "fd")

          println(s"FD processed: ${fd.toString}")
          fdsEncoded
            // .where(fdsEncoded(FullResult.attrnr) === HospSchema.getIndexesByAttrNames(fd1).head)
            .show(37, false)


        })


      }
    }


  }

  def plgrd_performEnsambleLearningOnToolsAndMetadata(session: SparkSession): Eval = {
    import session.implicits._

    val allColumns: Seq[String] = FullResult.tools

    val (trainFull, test) = new WranglingDatasetsToMetadata()
      .onDatasetName("hosp")
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

    //start: bayes
    val bayesModel = NaiveBayes.train(trainLabPointRDD, lambda = 1.0, modelType = "bernoulli")
    val predictByBayes = udf { features: org.apache.spark.ml.linalg.Vector => {
      val transformedFeatures = org.apache.spark.mllib.linalg.Vectors.dense(features.toArray)
      bayesModel.predict(transformedFeatures)
    }
    }

    val nbPrediction = dtPrediction.withColumn("nb-prediction", predictByBayes(dtPrediction(featuresCol)))
    val nbTrainPrediction = dtTrainPrediction.withColumn("nb-prediction", predictByBayes(dtTrainPrediction(featuresCol)))


    // meta classifier: logreg

    val assembler = new VectorAssembler()
      .setInputCols(Array("nn-prediction", "dt-prediction", "nb-prediction"))
      .setOutputCol("all-predictions")

    //Meta train
    val allTrainPrediction = assembler.transform(nbTrainPrediction).select(FullResult.label, "all-predictions")
    val allTrainClassifiers = allTrainPrediction.withColumnRenamed("all-predictions", featuresCol)

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

}


