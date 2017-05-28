package de.experiments.clustering

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN, Timer}
import de.experiments.ExperimentsCommonConfig
import de.experiments.models.combinator.{Bagging, Stacking}
import de.model.logistic.regression.ModelData
import de.model.util.{FormatUtil, ModelUtil}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, NaiveBayes}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable


/**
  * Created by visenger on 24/03/17.
  */
class ClusterAndCombineStrategy {

}

object ClusterAndCombineStrategyRunner extends ExperimentsCommonConfig {
  val rowIdCol = "row-id"
  val predictCol = "prediction"

  def main(args: Array[String]): Unit = {
    // val K = 4
    //    println("######## CLUSTERING ON THRUTH MATRIX:")
    //    (2 to K).foreach(k => runOnTruthMatrixAllClusters(k))
    //    println("######## CLUSTERING ON ERROR MATRIX:")
    //    (2 to K).foreach(k => runOnErrorMatrixAllClusters(k))
    //    println("######## CLUSTERING ON TRUTH MATRIX AND AGGREGATING TOOLS FROM THE BEST CLUSTER:")
    //    (2 to K).foreach(k => runOnBestCluster(k))

    //measureClustering()


    //runOnTruthMatrixPrintAllClusters()
     runOnTruthMatrixBestToolByCluster()

  }

  def measureClustering(): Unit = {
    Timer.measureRuntime(() => runOnTruthMatrixAllClusters())
  }

  def runOnTruthMatrixPrintAllClusters(): Unit = {
    SparkLOAN.withSparkSession("TOOLS-PER-CLUSTER") {
      session => {
        import session.implicits._
        process_data {
          data => {


            val dataSetName = data._1
            val trainFile = data._2._1
            val testFile = data._2._2

            println(s"CLUSTERING ON TRUTH MATRIX: $dataSetName")

            //            val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
            //            val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")


            val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
            val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

            val nxor = udf { (label: String, tool: String) => if (label.equals(tool)) "1" else "0" }
            val sum = udf { features: Vector => s"${features.numNonzeros} / ${features.size} " }
            // val getRealName = udf { alias: String => getExtName(alias) }

            var truthDF = trainDF
            val tools = FullResult.tools
            tools.foreach(tool => {
              truthDF = truthDF
                .withColumn(s"truth-$tool", nxor(trainDF("label"), trainDF(tool)))
            })

            val truthTools: Seq[String] = tools.map(t => s"truth-$t")

            var labelAndTruth = truthDF.select(FullResult.label, truthTools: _*)
            tools.foreach(tool => {
              labelAndTruth = labelAndTruth.withColumnRenamed(s"truth-$tool", tool)
            })

            /*TRANSPOSE MATRIX*/

            val columns: Seq[(String, Column)] = tools.map(t => (t, labelAndTruth(t)))

            val transposedDF: DataFrame = columns.map(column => {
              val columnName = column._1
              val columnForTool = labelAndTruth.select(column._2)
              val toolsVals: Array[Double] = columnForTool
                .rdd
                .map(element => element.getString(0).toDouble)
                .collect()
              val valsVector: Vector = Vectors.dense(toolsVals)
              (columnName, valsVector)
            }).toDF("tool-name", "features")

            //  transposedDF.withColumn(s"correct/total", sum(transposedDF("features"))).show()

            val indexer = new StringIndexer()
              .setInputCol("tool-name")
              .setOutputCol("label")

            val truthMatrixWithIndx = indexer
              .fit(transposedDF)
              .transform(transposedDF)

            //            val k = 3

            (2 to 4).foreach(k => {
              println(s"bisecting kMeans k = $k")
              // println(s"k = $k")

              /* val kMeans = new KMeans().setK(k)
               val kMeansModel = kMeans.fit(truthMatrixWithIndx)
               val kMeansClusters: DataFrame = kMeansModel
                 .transform(truthMatrixWithIndx)
                 .withColumn("tool", truthMatrixWithIndx("tool-name"))
                 .toDF()

               val kMeansResult: Seq[(Int, String)] = kMeansClusters
                 .select("prediction", "tool")
                 .groupByKey(row => {
                   row.getInt(0)
                 }).mapGroups((num, row) => {
                 val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
                 (num, clusterTools.mkString(splitter))
               }).rdd.collect().toSeq*/


              //hierarchical clustering
              val bisectingKMeans = new BisectingKMeans()
                .setSeed(5L)
                .setK(k)
              val bisectingKMeansModel = bisectingKMeans.fit(truthMatrixWithIndx)
              val bisectingKMeansClusters = bisectingKMeansModel
                .transform(truthMatrixWithIndx)
                .withColumn("tool", truthMatrixWithIndx("tool-name"))

              val bisectingKMeansResult: Seq[(Int, String)] = bisectingKMeansClusters
                .select("prediction", "tool")
                .groupByKey(row => {
                  row.getInt(0)
                }).mapGroups((num, row) => {
                val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
                (num, clusterTools.mkString(splitter))
              }).rdd.collect().toSeq


              /* we need row-id in order to join the prediction column with the core matrix*/
              // var testDataWithRowId: DataFrame = testDF.withColumn(rowIdCol, monotonically_increasing_id())

              var bestToolsByClusters: Seq[String] = Seq()

              bisectingKMeansResult.foreach(cluster => {
                val clusterTools: Seq[String] = cluster._2.split(splitter).toSeq
                println(s"tools per ${cluster._1+1;} cluster: ${clusterTools.map(getName(_)).mkString(" + ")}")
              })


            })

          }
        }

      }
    }
  }

  def runOnTruthMatrixBestToolByCluster(): Unit = {
    SparkLOAN.withSparkSession("BEST-TOOL-PER-CLUSTER") {
      session => {
        import session.implicits._
        process_data {
          data => {


            val dataSetName = data._1
            val trainFile = data._2._1
            val testFile = data._2._2

            println(s"CLUSTERING ON TRUTH MATRIX: $dataSetName")

            //            val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
            //            val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")


            val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
            val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

            val nxor = udf { (label: String, tool: String) => if (label.equals(tool)) "1" else "0" }
            val sum = udf { features: Vector => s"${features.numNonzeros} / ${features.size} " }
            // val getRealName = udf { alias: String => getExtName(alias) }

            var truthDF = trainDF
            val tools = FullResult.tools
            tools.foreach(tool => {
              truthDF = truthDF
                .withColumn(s"truth-$tool", nxor(trainDF("label"), trainDF(tool)))
            })

            val truthTools: Seq[String] = tools.map(t => s"truth-$t")

            var labelAndTruth = truthDF.select(FullResult.label, truthTools: _*)
            tools.foreach(tool => {
              labelAndTruth = labelAndTruth.withColumnRenamed(s"truth-$tool", tool)
            })

            /*TRANSPOSE MATRIX*/

            val columns: Seq[(String, Column)] = tools.map(t => (t, labelAndTruth(t)))

            val transposedDF: DataFrame = columns.map(column => {
              val columnName = column._1
              val columnForTool = labelAndTruth.select(column._2)
              val toolsVals: Array[Double] = columnForTool
                .rdd
                .map(element => element.getString(0).toDouble)
                .collect()
              val valsVector: Vector = Vectors.dense(toolsVals)
              (columnName, valsVector)
            }).toDF("tool-name", "features")

            //  transposedDF.withColumn(s"correct/total", sum(transposedDF("features"))).show()

            val indexer = new StringIndexer()
              .setInputCol("tool-name")
              .setOutputCol("label")

            val truthMatrixWithIndx = indexer
              .fit(transposedDF)
              .transform(transposedDF)

            //            val k = 3

            (2 to 4).foreach(k => {
              println(s"bisecting kMeans k = $k")
              // println(s"k = $k")

              /* val kMeans = new KMeans().setK(k)
               val kMeansModel = kMeans.fit(truthMatrixWithIndx)
               val kMeansClusters: DataFrame = kMeansModel
                 .transform(truthMatrixWithIndx)
                 .withColumn("tool", truthMatrixWithIndx("tool-name"))
                 .toDF()

               val kMeansResult: Seq[(Int, String)] = kMeansClusters
                 .select("prediction", "tool")
                 .groupByKey(row => {
                   row.getInt(0)
                 }).mapGroups((num, row) => {
                 val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
                 (num, clusterTools.mkString(splitter))
               }).rdd.collect().toSeq*/


              //hierarchical clustering
              val bisectingKMeans = new BisectingKMeans()
                .setSeed(5L)
                .setK(k)
              val bisectingKMeansModel = bisectingKMeans.fit(truthMatrixWithIndx)
              val bisectingKMeansClusters = bisectingKMeansModel
                .transform(truthMatrixWithIndx)
                .withColumn("tool", truthMatrixWithIndx("tool-name"))

              val bisectingKMeansResult: Seq[(Int, String)] = bisectingKMeansClusters
                .select("prediction", "tool")
                .groupByKey(row => {
                  row.getInt(0)
                }).mapGroups((num, row) => {
                val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
                (num, clusterTools.mkString(splitter))
              }).rdd.collect().toSeq


              /* we need row-id in order to join the prediction column with the core matrix*/
              // var testDataWithRowId: DataFrame = testDF.withColumn(rowIdCol, monotonically_increasing_id())

              var bestToolsByClusters: Seq[String] = Seq()

              bisectingKMeansResult.foreach(cluster => {
                val clusterTools: Seq[String] = cluster._2.split(splitter).toSeq
                println(s"tools per cluster: ")
              })

              /*Start Lin Combi on kMeansResult*/
              bisectingKMeansResult.foreach(cluster => {

                // val clusterNr = cluster._1
                val clusterTools: Seq[String] = cluster._2.split(splitter).toSeq

                /**
                  *
                  * experiments.conf
                  *
                  * exists-1.precision = 0.0024
                  * exists-2.precision = 0.0989
                  * exists-3.precision = 0.0326
                  * exists-4.precision = 0.1513
                  * exists-5.precision = 0.1313
                  *
                  * todo: select max percision [datasetname].[toolname].precision
                  *
                  **/


                val toolsToPrecisions: Map[String, Double] = clusterTools.map(toolStr => {

                  toolStr -> experimentsConf.getDouble(s"$dataSetName.$toolStr.precision")
                }).toMap
                val maxPrecision = toolsToPrecisions.values.max
                val bestTool: String = toolsToPrecisions.filter(entry => {
                  val precision = entry._2
                  precision.equals(maxPrecision)
                }).head._1
                bestToolsByClusters = bestToolsByClusters ++ Seq(bestTool)

              })

              println(s" $dataSetName $k-best tools: ${bestToolsByClusters.map(getName(_)).mkString(splitter)}")

              val clustersBagging = new Bagging()
              val evalClustering = clustersBagging
                .onDataSetName(dataSetName)
                .useTools(bestToolsByClusters)
                .onTrainDataFrame(trainDF)
                .onTestDataFrame(testDF)
                .performEnsambleLearningOnTools(session)

              // evalClustering.printResult(s"BAGGING: CLUSTER AND AGGREGATE BEST TOOLS ON $dataSetName")

              val baggingWithMeta = new Bagging()
              val evalMetaBaggingOnClusters = baggingWithMeta
                .onDataSetName(dataSetName)
                .useTools(bestToolsByClusters)
                .onTrainDataFrame(trainDF)
                .onTestDataFrame(testDF)
                .performEnsambleLearningOnToolsAndMetadata(session)
              // evalMetaBaggingOnClusters.printResult(s"BAGGING & METADATA: CLUSTER AND AGGREGATE BEST TOOLS ON $dataSetName")
              println(s"bagging: ${k}\t${evalClustering.precision}\t${evalClustering.recall}\t${evalClustering.f1}\t${evalMetaBaggingOnClusters.precision}\t${evalMetaBaggingOnClusters.recall}\t${evalMetaBaggingOnClusters.f1}")

              val stacking = new Stacking()
              val evalClustWithStacking = stacking
                .onDataSetName(dataSetName)
                .useTools(bestToolsByClusters)
                .onTrainDataFrame(trainDF)
                .onTestDataFrame(testDF)
                .performEnsambleLearningOnTools(session)
              //              evalClustWithStacking.printResult(s"STACKING: CLUSTER AND AGGREGATE BEST TOOLS ON $dataSetName")


              val stackingWithMeta = new Stacking()
              val evalMetaStackingOnClusters = stackingWithMeta
                .onDataSetName(dataSetName)
                .useTools(bestToolsByClusters)
                .onTrainDataFrame(trainDF)
                .onTestDataFrame(testDF)
                .performEnsambleLearningOnToolsAndMetadata(session)
              //              evalMetaStackingOnClusters.printResult(s"STACKING & METADATA: CLUSTER AND AGGREGATE BEST TOOLS ON $dataSetName")
              println(s"stacking: $k\t${evalClustWithStacking.precision}\t${evalClustWithStacking.recall}\t${evalClustWithStacking.f1}\t${evalMetaStackingOnClusters.precision}\t${evalMetaStackingOnClusters.recall}\t${evalMetaStackingOnClusters.f1}")

            })

          }
        }

      }
    }
  }

  def runOnTruthMatrixAllClusters(k: Int = 3): Unit = {

    SparkLOAN.withSparkSession("WAHRHEITSMATRIX") {
      session => {
        import session.implicits._

        process_data {
          data => {
            val dataSetName = data._1
            val trainFile = data._2._1
            val testFile = data._2._2

            println(s"CLUSTERING ON TRUTH MATRIX: $dataSetName")

            val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
            val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")


            val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
            val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

            val nxor = udf { (label: String, tool: String) => if (label.equals(tool)) "1" else "0" }
            val sum = udf { features: Vector => s"${features.numNonzeros} / ${features.size} " }
            val getRealName = udf { alias: String => getExtName(alias) }

            var truthDF = trainDF
            val tools = FullResult.tools
            tools.foreach(tool => {
              truthDF = truthDF
                .withColumn(s"truth-$tool", nxor(trainDF("label"), trainDF(tool)))
            })

            val truthTools: Seq[String] = tools.map(t => s"truth-$t")

            var labelAndTruth = truthDF.select(FullResult.label, truthTools: _*)
            tools.foreach(tool => {
              labelAndTruth = labelAndTruth.withColumnRenamed(s"truth-$tool", tool)
            })

            /*TRANSPOSE MATRIX*/

            val columns: Seq[(String, Column)] = tools.map(t => (t, labelAndTruth(t)))

            val transposedDF: DataFrame = columns.map(column => {
              val columnName = column._1
              val columnForTool = labelAndTruth.select(column._2)
              val toolsVals: Array[Double] = columnForTool
                .rdd
                .map(element => element.getString(0).toDouble)
                .collect()
              val valsVector: Vector = Vectors.dense(toolsVals)
              (columnName, valsVector)
            }).toDF("tool-name", "features")

            transposedDF.withColumn(s"correct/total", sum(transposedDF("features"))).show()

            val indexer = new StringIndexer()
              .setInputCol("tool-name")
              .setOutputCol("label")

            val truthMatrixWithIndx = indexer
              .fit(transposedDF)
              .transform(transposedDF)

            //            val k = 3
            println(s"bisecting kMeans")
            println(s"k = $k")

            /* val kMeans = new KMeans().setK(k)
             val kMeansModel = kMeans.fit(truthMatrixWithIndx)
             val kMeansClusters: DataFrame = kMeansModel
               .transform(truthMatrixWithIndx)
               .withColumn("tool", truthMatrixWithIndx("tool-name"))
               .toDF()

             val kMeansResult: Seq[(Int, String)] = kMeansClusters
               .select("prediction", "tool")
               .groupByKey(row => {
                 row.getInt(0)
               }).mapGroups((num, row) => {
               val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
               (num, clusterTools.mkString(splitter))
             }).rdd.collect().toSeq*/


            //hierarchical clustering
            val bisectingKMeans = new BisectingKMeans()
              .setSeed(5L)
              .setK(k)
            val bisectingKMeansModel = bisectingKMeans.fit(truthMatrixWithIndx)
            val bisectingKMeansClusters = bisectingKMeansModel
              .transform(truthMatrixWithIndx)
              .withColumn("tool", truthMatrixWithIndx("tool-name"))

            val bisectingKMeansResult: Seq[(Int, String)] = bisectingKMeansClusters
              .select("prediction", "tool")
              .groupByKey(row => {
                row.getInt(0)
              }).mapGroups((num, row) => {
              val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
              (num, clusterTools.mkString(splitter))
            }).rdd.collect().toSeq


            /* we need row-id in order to join the prediction column with the core matrix*/
            var testDataWithRowId: DataFrame = testDF.withColumn(rowIdCol, monotonically_increasing_id())

            var allClusterColumns: Seq[String] = Seq()

            /*Start Lin Combi on kMeansResult*/
            bisectingKMeansResult.foreach(cluster => {

              val clusterNr = cluster._1
              val clusterTools: Seq[String] = cluster._2.split(splitter).toSeq

              val train = FormatUtil.prepareDataToLabeledPoints(session, trainDF, clusterTools)

              val Array(train1, _) = train.randomSplit(Array(0.7, 0.3), seed = 123L)
              val Array(_, train2) = train.randomSplit(Array(0.3, 0.7), seed = 23L)
              val Array(train3, _) = train.randomSplit(Array(0.7, 0.3), seed = 593L)
              val Array(_, train4) = train.randomSplit(Array(0.3, 0.7), seed = 941L)
              val Array(train5, _) = train.randomSplit(Array(0.7, 0.3), seed = 3L)
              val Array(_, train6) = train.randomSplit(Array(0.3, 0.7), seed = 623L)

              val trainSamples = Seq(train1, train2, train3, train4, train5, train6)

              val Array(model1, model2, model3, model4, model5, model6) = getDecisionTreeModels(trainSamples, clusterTools).toArray

              var labeledPointsDF: DataFrame = prepareDataToIdWithFeatures(session, testDataWithRowId, clusterTools)

              val bagging1 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model1.predict(features) }
              val bagging2 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model2.predict(features) }
              val bagging3 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model3.predict(features) }
              val bagging4 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model4.predict(features) }
              val bagging5 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model5.predict(features) }
              val bagging6 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model6.predict(features) }

              val featuresCol = "features"
              labeledPointsDF = labeledPointsDF
                .withColumn(s"model-1", bagging1(labeledPointsDF(featuresCol)))
                .withColumn(s"model-2", bagging2(labeledPointsDF(featuresCol)))
                .withColumn(s"model-3", bagging3(labeledPointsDF(featuresCol)))
                .withColumn(s"model-4", bagging4(labeledPointsDF(featuresCol)))
                .withColumn(s"model-5", bagging5(labeledPointsDF(featuresCol)))
                .withColumn(s"model-6", bagging6(labeledPointsDF(featuresCol)))

              // Majority wins
              val majorityVoter = udf { (tools: mutable.WrappedArray[Double]) => {
                val total = tools.length
                val sum1 = tools.count(_ == 1.0)
                val sum0 = total - sum1
                val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
                errorDecision
              }
              }

              val clusterCol = s"cluster-$clusterNr"
              val baggingColumns = (1 to trainSamples.size).map(id => labeledPointsDF(s"model-${id}"))
              val voter = labeledPointsDF
                .withColumn(clusterCol, majorityVoter(array(baggingColumns: _*)))

              //eval cluster: start
              val predictionAndLabel = FormatUtil.getPredictionAndLabel(voter, clusterCol)
              val evalCluster = F1.evalPredictionAndLabels(predictionAndLabel)
              evalCluster.printResult(s"$clusterCol eval")
              //eval cluster: finish

              val clusterPrediction: DataFrame = voter
                .select(rowIdCol, clusterCol)

              testDataWithRowId = testDataWithRowId.join(clusterPrediction, rowIdCol)
              allClusterColumns = allClusterColumns ++ Seq(clusterCol)

            })

            val featuresCol = "features"
            println(s"all clusters: ${allClusterColumns.mkString(", ")}")

            val selectCols = Seq(FullResult.label) ++ allClusterColumns

            val Array(clustTrain, clustTest) = testDataWithRowId
              .select(rowIdCol, selectCols: _*)
              .randomSplit(Array(0.2, 0.8))

            val trainLabeled: RDD[LabeledPoint] = FormatUtil.prepareClustersToLabeledPoints(session, clustTrain, allClusterColumns)
            val testLabeled: RDD[LabeledPoint] = FormatUtil.prepareClustersToLabeledPoints(session, clustTest, allClusterColumns)

            val (bestFinalModelData, bestFinalModel) = getBestModel(maxPrecision, maxRecall, trainLabeled, testLabeled)

            val threshold = bestFinalModelData.bestThreshold
            bestFinalModel.setThreshold(threshold)
            println(s"best threshold for the final model:$threshold")

            val logRegression = udf { features: org.apache.spark.mllib.linalg.Vector => bestFinalModel.predict(features) }
            val finalTestWithFeatures: DataFrame = prepareClustersToIdWithFeatures(session, clustTest, allClusterColumns)

            val finalPredictCol = "log-reg"
            val predictAndLabel: RDD[(Double, Double)] = finalTestWithFeatures
              .withColumn(finalPredictCol, logRegression(finalTestWithFeatures(featuresCol)))
              .select(finalPredictCol, FullResult.label)
              .rdd
              .map(row => (row.getDouble(0), row.getDouble(1)))

            val eval = F1.evalPredictionAndLabels(predictAndLabel)
            eval.printResult("final log regr")

            //start:neural networks
            val clusterNums = allClusterColumns.size
            val nextLayer = clusterNums + 1
            val numClasses = 2
            val layers = Array[Int](clusterNums, nextLayer, clusterNums, numClasses)
            val trainer = new MultilayerPerceptronClassifier()
              .setLayers(layers)
              .setBlockSize(128)
              .setSeed(1234L)
              .setMaxIter(100)
            val nnTrain = FormatUtil.prepareDoublesToLIBSVM(session, clustTrain, allClusterColumns)
            val networkModel = trainer.fit(nnTrain)
            val nnTest = FormatUtil.prepareDoublesToLIBSVM(session, clustTest, allClusterColumns)
            val result = networkModel.transform(nnTest)
            //eval
            val nnPrediction = result.select(FullResult.label, predictCol)
            val predictAndLabelNN = FormatUtil.getPredictionAndLabel(nnPrediction, predictCol)
            val evalNN = F1.evalPredictionAndLabels(predictAndLabelNN)
            evalNN.printResult("final neural networks on clusters")
            //end:neural networks


            //start: Naive Bayes for classifier combination:
            val naiveBayesModel = NaiveBayes.train(trainLabeled, lambda = 0.5, modelType = "bernoulli")
            val predictByNaiveBayes = udf { features: org.apache.spark.mllib.linalg.Vector => {
              naiveBayesModel.predict(features)
            }
            }

            val bayesOfAllCombi: RDD[(Double, Double)] = finalTestWithFeatures
              .withColumn("bayes-of-combi", predictByNaiveBayes(finalTestWithFeatures(featuresCol)))
              .select(FullResult.label, "bayes-of-combi")
              .rdd.map(row => {
              val label = row.getDouble(0)
              val prediction = row.getDouble(1)
              (prediction, label)
            })

            val evalCombiBayes = F1.evalPredictionAndLabels(bayesOfAllCombi)
            evalCombiBayes.printResult("bayes combi on clusters")
            //end: Naive Bayes for classifier combination

            //start:DecisionTree for classifier combi;
            val featuresNumber = allClusterColumns.size
            val toolsFeaturesInfo: Map[Int, Int] = (0 until featuresNumber)
              .map(attr => attr -> 2)
              .toMap // Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2)
            // val impurityFinal = "entropy"
            val mDepth = featuresNumber
            val model = DecisionTree.trainClassifier(trainLabeled, numClasses, toolsFeaturesInfo,
              impurity = "gini", mDepth, maxBins = 32)

            val predictByDT = udf { features: org.apache.spark.mllib.linalg.Vector => {
              model.predict(features)
            }
            }

            val dtCol = "decision-tree"
            val dtPrediction = finalTestWithFeatures
              .withColumn(dtCol, predictByDT(finalTestWithFeatures(featuresCol)))
              .select(FullResult.label, dtCol)

            val predictionAndLabelDT = FormatUtil.getPredictionAndLabel(dtPrediction, dtCol)
            val evalDT = F1.evalPredictionAndLabels(predictionAndLabelDT)
            evalDT.printResult(s"decision tree on clusters")
            //end:DecisionTree for classifier combi;
          }
        }
      }
    }
  }

  def runOnErrorMatrixAllClusters(k: Int = 3): Unit = {

    SparkLOAN.withSparkSession("ERROR-MATRIX") {
      session => {

        process_data {
          data => {
            val dataSetName = data._1
            val trainFile = data._2._1
            val testFile = data._2._2

            println(s"CLUSTERING ON ERROR MATRIX: $dataSetName")

            val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
            val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

            val maximumF1 = experimentsConf.getDouble(s"${dataSetName}.max.F1")
            val minimumF1 = experimentsConf.getDouble(s"${dataSetName}.min.F1")

            import session.implicits._

            val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
            val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)


            val sum = udf { features: Vector => s"${features.numNonzeros} / ${features.size} " }
            val getRealName = udf { alias: String => getExtName(alias) }

            //            tools.foreach(tool => {
            //              truthDF = truthDF
            //                .withColumn(s"truth-$tool", nxor(trainDF("label"), trainDF(tool)))
            //            })

            //            val truthTools: Seq[String] = tools.map(t => s"truth-$t")
            //
            //
            //            var labelAndTruth = truthDF.select(FullResult.label, truthTools: _*)
            //            tools.foreach(tool => {
            //              labelAndTruth = labelAndTruth.withColumnRenamed(s"truth-$tool", tool)
            //            })

            /*TRANSPOSE MATRIX*/

            val columns: Seq[(String, Column)] = FullResult.tools.map(t => (t, trainDF(t)))

            val transposedDF: DataFrame = columns.map(column => {
              val columnName = column._1
              val columnForTool = trainDF.select(column._2)
              val toolsVals: Array[Double] = columnForTool
                .rdd
                .map(element => element.getString(0).toDouble)
                .collect()
              val valsVector: Vector = Vectors.dense(toolsVals)
              (columnName, valsVector)
            }).toDF("tool-name", "features")


            transposedDF.withColumn(s"error/total", sum(transposedDF("features"))).show()

            val indexer = new StringIndexer()
              .setInputCol("tool-name")
              .setOutputCol("label")

            val truthMatrixWithIndx = indexer
              .fit(transposedDF)
              .transform(transposedDF)

            println(s"kMeans")
            println(s"k = $k")

            /* val kMeans = new KMeans().setK(k)
             val kMeansModel = kMeans.fit(truthMatrixWithIndx)
             val kMeansClusters: DataFrame = kMeansModel
               .transform(truthMatrixWithIndx)
               .withColumn("tool", truthMatrixWithIndx("tool-name"))
               .toDF()

             val kMeansResult: Seq[(Int, String)] = kMeansClusters
               .select("prediction", "tool")
               .groupByKey(row => {
                 row.getInt(0)
               }).mapGroups((num, row) => {
               val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
               (num, clusterTools.mkString(splitter))
             }).rdd.collect().toSeq*/


            //hierarchical clustering
            val bisectingKMeans = new BisectingKMeans()
              .setSeed(5L)
              .setK(k)
            val bisectingKMeansModel = bisectingKMeans.fit(truthMatrixWithIndx)
            val bisectingKMeansClusters = bisectingKMeansModel
              .transform(truthMatrixWithIndx)
              .withColumn("tool", truthMatrixWithIndx("tool-name"))

            val bisectingKMeansResult: Seq[(Int, String)] = bisectingKMeansClusters
              .select("prediction", "tool")
              .groupByKey(row => {
                row.getInt(0)
              }).mapGroups((num, row) => {
              val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
              (num, clusterTools.mkString(splitter))
            }).rdd.collect().toSeq


            /* we need row-id in order to join the prediction column with the core matrix*/
            var testDataWithRowId: DataFrame = testDF.withColumn(rowIdCol, monotonically_increasing_id())

            var allClusterColumns: Seq[String] = Seq()

            /*Start Lin Combi on kMeansResult*/
            bisectingKMeansResult.foreach(cluster => {

              val clusterNr = cluster._1
              val clusterTools: Seq[String] = cluster._2.split(splitter).toSeq

              val train = FormatUtil.prepareDataToLabeledPoints(session, trainDF, clusterTools)

              val Array(train1, _) = train.randomSplit(Array(0.7, 0.3), seed = 123L)
              val Array(_, train2) = train.randomSplit(Array(0.3, 0.7), seed = 23L)
              val Array(train3, _) = train.randomSplit(Array(0.7, 0.3), seed = 593L)
              val Array(_, train4) = train.randomSplit(Array(0.3, 0.7), seed = 941L)
              val Array(train5, _) = train.randomSplit(Array(0.7, 0.3), seed = 3L)
              val Array(_, train6) = train.randomSplit(Array(0.3, 0.7), seed = 623L)

              val trainSamples = Seq(train1, train2, train3, train4, train5, train6)

              val Array(model1, model2, model3, model4, model5, model6) = getDecisionTreeModels(trainSamples, clusterTools).toArray

              var labeledPointsDF: DataFrame = prepareDataToIdWithFeatures(session, testDataWithRowId, clusterTools)

              val labeledPointCol = "features"
              val clusterCol = s"cluster-$clusterNr"

              val bagging1 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model1.predict(features) }
              val bagging2 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model2.predict(features) }
              val bagging3 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model3.predict(features) }
              val bagging4 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model4.predict(features) }
              val bagging5 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model5.predict(features) }
              val bagging6 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model6.predict(features) }

              labeledPointsDF = labeledPointsDF
                .withColumn(s"model-1", bagging1(labeledPointsDF(labeledPointCol)))
                .withColumn(s"model-2", bagging2(labeledPointsDF(labeledPointCol)))
                .withColumn(s"model-3", bagging3(labeledPointsDF(labeledPointCol)))
                .withColumn(s"model-4", bagging4(labeledPointsDF(labeledPointCol)))
                .withColumn(s"model-5", bagging5(labeledPointsDF(labeledPointCol)))
                .withColumn(s"model-6", bagging6(labeledPointsDF(labeledPointCol)))

              // Majority wins
              val majorityVoter = udf { (tools: mutable.WrappedArray[Double]) => {
                val total = tools.length
                val sum1 = tools.count(_ == 1.0)
                val sum0 = total - sum1
                val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
                errorDecision
              }
              }

              val baggingColumns = (1 to trainSamples.size).map(id => labeledPointsDF(s"model-${id}"))
              val voter = labeledPointsDF
                .withColumn(clusterCol, majorityVoter(array(baggingColumns: _*)))

              //eval cluster: start
              val predictionAndLabel = FormatUtil.getPredictionAndLabel(voter, clusterCol)
              val evalCluster = F1.evalPredictionAndLabels(predictionAndLabel)
              evalCluster.printResult(s"$clusterCol eval")
              //eval cluster: finish

              val clusterPrediction: DataFrame = voter
                .select(rowIdCol, clusterCol)

              testDataWithRowId = testDataWithRowId.join(clusterPrediction, rowIdCol)
              allClusterColumns = allClusterColumns ++ Seq(clusterCol)

            })

            val featuresCol = "features"
            println(s"all clusters: ${allClusterColumns.mkString(", ")}")

            val selectCols = Seq(FullResult.label) ++ allClusterColumns

            val Array(clustTrain, clustTest) = testDataWithRowId
              .select(rowIdCol, selectCols: _*)
              .randomSplit(Array(0.2, 0.8))

            val trainLabeled: RDD[LabeledPoint] = FormatUtil.prepareClustersToLabeledPoints(session, clustTrain, allClusterColumns)
            val testLabeled: RDD[LabeledPoint] = FormatUtil.prepareClustersToLabeledPoints(session, clustTest, allClusterColumns)

            val (bestFinalModelData, bestFinalModel) = getBestModel(maxPrecision, maxRecall, trainLabeled, testLabeled)

            val threshold = bestFinalModelData.bestThreshold
            bestFinalModel.setThreshold(threshold)
            println(s"best threshold for the final model:${threshold}")

            val logRegression = udf { features: org.apache.spark.mllib.linalg.Vector => bestFinalModel.predict(features) }
            val finalTestWithFeatures: DataFrame = prepareClustersToIdWithFeatures(session, clustTest, allClusterColumns)

            val finalPredictCol = "log-reg"
            val predictAndLabel: RDD[(Double, Double)] = finalTestWithFeatures
              .withColumn(finalPredictCol, logRegression(finalTestWithFeatures(featuresCol)))
              .select(finalPredictCol, FullResult.label)
              .rdd
              .map(row => (row.getDouble(0), row.getDouble(1)))

            val eval = F1.evalPredictionAndLabels(predictAndLabel)
            eval.printResult("final log regression: ")

            //start:neural networks
            val clusterNums = allClusterColumns.size
            val nextLayer = clusterNums + 1
            val numClasses = 2
            val layers = Array[Int](clusterNums, nextLayer, clusterNums, numClasses)
            val trainer = new MultilayerPerceptronClassifier()
              .setLayers(layers)
              .setBlockSize(128)
              .setSeed(1234L)
              .setMaxIter(100)
            val nnTrain = FormatUtil.prepareDoublesToLIBSVM(session, clustTrain, allClusterColumns)
            val networkModel = trainer.fit(nnTrain)
            val nnTest = FormatUtil.prepareDoublesToLIBSVM(session, clustTest, allClusterColumns)
            val result = networkModel.transform(nnTest)
            //eval
            val nnPrediction = result.select(FullResult.label, predictCol)
            val predictAndLabelNN = FormatUtil.getPredictionAndLabel(nnPrediction, predictCol)
            val evalNN = F1.evalPredictionAndLabels(predictAndLabelNN)
            evalNN.printResult("final neural networks on clusters")
            //end:neural networks


            //start: Naive Bayes for classifier combination:
            val naiveBayesModel = NaiveBayes.train(trainLabeled, lambda = 0.5, modelType = "bernoulli")
            val predictByNaiveBayes = udf { features: org.apache.spark.mllib.linalg.Vector => {
              naiveBayesModel.predict(features)
            }
            }

            val bayesOfAllCombi: RDD[(Double, Double)] = finalTestWithFeatures
              .withColumn("bayes-of-combi", predictByNaiveBayes(finalTestWithFeatures(featuresCol)))
              .select(FullResult.label, "bayes-of-combi")
              .rdd.map(row => {
              val label = row.getDouble(0)
              val prediction = row.getDouble(1)
              (prediction, label)
            })

            val evalCombiBayes = F1.evalPredictionAndLabels(bayesOfAllCombi)
            evalCombiBayes.printResult("bayes combi on clusters")
            //end: Naive Bayes for classifier combination

            //start:DecisionTree for classifier combi;
            val featuresNumber = allClusterColumns.size
            val toolsFeaturesInfo: Map[Int, Int] = (0 until featuresNumber)
              .map(attr => attr -> 2)
              .toMap // Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2)
            // val impurityFinal = "entropy"
            val mDepth = featuresNumber
            val model = DecisionTree.trainClassifier(trainLabeled, numClasses, toolsFeaturesInfo,
              impurity = "gini", mDepth, maxBins = 32)

            val predictByDT = udf { features: org.apache.spark.mllib.linalg.Vector => {
              model.predict(features)
            }
            }

            val dtCol = "decision-tree"
            val dtPrediction = finalTestWithFeatures
              .withColumn(dtCol, predictByDT(finalTestWithFeatures(featuresCol)))
              .select(FullResult.label, dtCol)

            val predictionAndLabelDT = FormatUtil.getPredictionAndLabel(dtPrediction, dtCol)
            val evalDT = F1.evalPredictionAndLabels(predictionAndLabelDT)
            evalDT.printResult(s"decision tree on clusters")
            //end:DecisionTree for classifier combi;


          }
        }


      }
    }
  }

  def getDecisionTreeModels(trainSamples: Seq[RDD[LabeledPoint]], clusterTools: Seq[String]): Seq[DecisionTreeModel] = {
    val numClasses = 2
    val toolsNum = clusterTools.size
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


  /*def _first_version_runOnAllClusters(): Unit = {

    SparkLOAN.withSparkSession("WAHRHEITSMATRIX") {
      session => {

        println(s"CLUSTERING ON TRUTH MATRIX")

        //todo: extract to commons
        val dataSetName = "ext.blackoak"
        val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
        val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

        val maximumF1 = experimentsConf.getDouble(s"${dataSetName}.max.F1")
        val minimumF1 = experimentsConf.getDouble(s"${dataSetName}.min.F1")

        import org.apache.spark.sql.functions._
        import session.implicits._

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        val testDF = DataSetCreator.createFrame(session, extBlackoakTestFile, FullResult.schema: _*)

        /* we need row-id in order to join the prediction column with the */
        import org.apache.spark.sql.functions.monotonically_increasing_id
        var testDataWithRowId: DataFrame = testDF.withColumn(rowIdCol, monotonically_increasing_id())

        val nxor = udf { (label: String, tool: String) => if (label.equals(tool)) "1" else "0" }
        val sum = udf { features: Vector => s"${features.numNonzeros} / ${features.size} " }
        val getRealName = udf { alias: String => getExtName(alias) }

        var truthDF = trainDF
        val tools = FullResult.tools
        tools.foreach(tool => {
          truthDF = truthDF
            .withColumn(s"truth-$tool", nxor(trainDF("label"), trainDF(tool)))
        })

        val truthTools: Seq[String] = tools.map(t => s"truth-$t")


        var labelAndTruth = truthDF.select(FullResult.label, truthTools: _*)
        tools.foreach(tool => {
          labelAndTruth = labelAndTruth.withColumnRenamed(s"truth-$tool", tool)
        })

        /*TRANSPOSE MATRIX*/

        val columns: Seq[(String, Column)] = tools.map(t => (t, labelAndTruth(t)))

        val transposedDF: DataFrame = columns.map(column => {
          val columnName = column._1
          val columnForTool = labelAndTruth.select(column._2)
          val toolsVals: Array[Double] = columnForTool
            .rdd
            .map(element => element.getString(0).toDouble)
            .collect()
          val valsVector: Vector = Vectors.dense(toolsVals)
          (columnName, valsVector)
        }).toDF("tool-name", "features")


        transposedDF.withColumn(s"correct/total", sum(transposedDF("features"))).show()

        val indexer = new StringIndexer()
          .setInputCol("tool-name")
          .setOutputCol("label")

        val truthMatrixWithIndx = indexer
          .fit(transposedDF)
          .transform(transposedDF)

        val k = 3
        println(s" kMeans")
        println(s"k = $k")

        val kMeans = new KMeans().setK(k)
        val kMeansModel = kMeans.fit(truthMatrixWithIndx)
        val kMeansClusters: DataFrame = kMeansModel
          .transform(truthMatrixWithIndx)
          .withColumn("tool", truthMatrixWithIndx("tool-name"))
          .toDF()

        val kMeansResult: Seq[(Int, String)] = kMeansClusters
          .select("prediction", "tool")
          .groupByKey(row => {
            row.getInt(0)
          }).mapGroups((num, row) => {
          val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
          (num, clusterTools.mkString(splitter))
        }).rdd.collect().toSeq


        /*Start Lin Combi on kMeansResult*/
        kMeansResult.foreach(cluster => {

          val clusterNr = cluster._1
          val clusterTools: Seq[String] = cluster._2.split(splitter).toSeq

          val train = FormatUtil.prepareDataToLabeledPoints(session, trainDF, clusterTools)
          val test = FormatUtil.prepareDataToLabeledPoints(session, testDF, clusterTools)

          val (modelData, model) = getBestModel(maxPrecision, maxRecall, train, test)

          //model.setThreshold(modelData.bestThreshold)
          model.clearThreshold()
          println(s"cluster nr: $clusterNr -> ${cluster._2} $modelData")

          val labeledPointsDF: DataFrame = prepareDataToIdWithFeatures(session, testDataWithRowId, clusterTools)

          val labeledPointCol = "features"
          val clusterCol = s"cluster-$clusterNr"

          val predictLabel = udf { features: org.apache.spark.mllib.linalg.Vector => model.predict(features) }
          val clusterPrediction: DataFrame = labeledPointsDF
            .withColumn(clusterCol, predictLabel(labeledPointsDF(labeledPointCol)))
            .select(rowIdCol, clusterCol)

          testDataWithRowId = testDataWithRowId.join(clusterPrediction, rowIdCol)
        })
        /*Finish Lin Combi*/

        testDataWithRowId.printSchema()

        testDataWithRowId.show()

        val allClusterColumns: Seq[String] = (0 until k).map(num => s"cluster-${num}")
        val selectCols = Seq(FullResult.label) ++ allClusterColumns

        val Array(clustTrain, clustTest) = testDataWithRowId
          .select(rowIdCol, selectCols: _*)
          .randomSplit(Array(0.2, 0.8))

        val trainLabeled: RDD[LabeledPoint] = FormatUtil.prepareClustersToLabeledPoints(session, clustTrain, allClusterColumns)
        val testLabeled: RDD[LabeledPoint] = FormatUtil.prepareClustersToLabeledPoints(session, clustTest, allClusterColumns)

        val (bestFinalModelData, bestFinalModel) = getBestModel(maxPrecision, maxRecall, trainLabeled, testLabeled)

        val threshold = bestFinalModelData.bestThreshold
        bestFinalModel.setThreshold(threshold)
        //bestFinalModel.setThreshold(bestFinalModelData.bestThreshold)
        println(s"best threshold for the final model:${threshold}")

        val finalPredict = udf { features: org.apache.spark.mllib.linalg.Vector => bestFinalModel.predict(features) }
        val finalTestWithFeatures: DataFrame = prepareClustersToIdWithFeatures(session, clustTest, allClusterColumns)

        val finalPredictCol = "final-predict"

        val predictAndLabel: RDD[(Double, Double)] = finalTestWithFeatures
          .withColumn(finalPredictCol, finalPredict(finalTestWithFeatures("features")))
          .select(finalPredictCol, FullResult.label)
          .rdd
          .map(row => (row.getDouble(0), row.getDouble(1)))

        val eval = F1.evalPredictionAndLabels(predictAndLabel)
        eval.printResult("CLUSTERING AND LINEAR COMBI: ")


        /*//hierarchical clustering
        val bisectingKMeans = new BisectingKMeans()
          //.setSeed(5L)
          .setK(k)
        val bisectingKMeansModel = bisectingKMeans.fit(truthMatrixWithIndx)
        val bisectingKMeansClusters = bisectingKMeansModel
          .transform(truthMatrixWithIndx)
          .withColumn("tool", truthMatrixWithIndx("tool-name"))


        val bisectingKMeansResult: Seq[(Int, String)] = bisectingKMeansClusters
          .select("prediction", "tool")
          .groupByKey(row => {
            row.getInt(0)
          }).mapGroups((num, row) => {
          val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
          (num, clusterTools.mkString(splitter))
        }).rdd.collect().toSeq

        println(s"bisecting kMeans")
        bisectingKMeansResult.foreach(cluster => {
          val evals: String = evaluateCluster(session, dataSetName, trainDF, cluster)
          println(evals)

        })*/


      }
    }
  }*/


  def runOnBestCluster(k: Int = 3): Unit = {

    SparkLOAN.withSparkSession("WAHRHEITSMATRIX") {
      session => {
        import session.implicits._

        process_data {
          data => {
            val dataSetName = data._1
            val trainFile = data._2._1
            val testFile = data._2._2

            println(s"BEST CLUSTER SOLUTION: CLUSTERING ON TRUTH MATRIX FOR $dataSetName")

            val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
            val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

            val maximumF1 = experimentsConf.getDouble(s"${dataSetName}.max.F1")
            val minimumF1 = experimentsConf.getDouble(s"${dataSetName}.min.F1")

            val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
            val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

            val nxor = udf { (label: String, tool: String) => if (label.equals(tool)) "1" else "0" }
            val sum = udf { features: Vector => s"${features.numNonzeros} / ${features.size} " }
            val getRealName = udf { alias: String => getExtName(alias) }

            var truthDF = trainDF
            val tools = FullResult.tools
            tools.foreach(tool => {
              truthDF = truthDF
                .withColumn(s"truth-$tool", nxor(trainDF("label"), trainDF(tool)))
            })

            val truthTools: Seq[String] = tools.map(t => s"truth-$t")

            var labelAndTruth = truthDF.select(FullResult.label, truthTools: _*)
            tools.foreach(tool => {
              labelAndTruth = labelAndTruth.withColumnRenamed(s"truth-$tool", tool)
            })

            /*TRANSPOSE MATRIX*/

            val columns: Seq[(String, Column)] = tools.map(t => (t, labelAndTruth(t)))

            val transposedDF: DataFrame = columns.map(column => {
              val columnName = column._1
              val columnForTool = labelAndTruth.select(column._2)
              val toolsVals: Array[Double] = columnForTool
                .rdd
                .map(element => element.getString(0).toDouble)
                .collect()
              val valsVector: Vector = Vectors.dense(toolsVals)
              (columnName, valsVector)
            }).toDF("tool-name", "features")

            transposedDF.withColumn(s"correct/total", sum(transposedDF("features"))).show()

            val indexer = new StringIndexer()
              .setInputCol("tool-name")
              .setOutputCol("label")

            val truthMatrixWithIndx = indexer
              .fit(transposedDF)
              .transform(transposedDF)

            println(s" kMeans")
            println(s"k = $k")

            /* val kMeans = new KMeans().setK(k)
             val kMeansModel = kMeans.fit(truthMatrixWithIndx)
             val kMeansClusters: DataFrame = kMeansModel
               .transform(truthMatrixWithIndx)
               .withColumn("tool", truthMatrixWithIndx("tool-name"))
               .toDF()

             val kMeansResult: Seq[(Int, String)] = kMeansClusters
               .select("prediction", "tool")
               .groupByKey(row => {
                 row.getInt(0)
               }).mapGroups((num, row) => {
               val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
               (num, clusterTools.mkString(splitter))
             }).rdd.collect().toSeq
 */
            //hierarchical clustering
            val bisectingKMeans = new BisectingKMeans()
              .setSeed(5L)
              .setK(k)
            val bisectingKMeansModel = bisectingKMeans.fit(truthMatrixWithIndx)
            val bisectingKMeansClusters = bisectingKMeansModel
              .transform(truthMatrixWithIndx)
              .withColumn("tool", truthMatrixWithIndx("tool-name"))

            val bisectingKMeansResult: Seq[(Int, String)] = bisectingKMeansClusters
              .select("prediction", "tool")
              .groupByKey(row => {
                row.getInt(0)
              }).mapGroups((num, row) => {
              val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
              (num, clusterTools.mkString(splitter))
            }).rdd.collect().toSeq


            var maxF1 = 0.0
            var bestTools: Seq[String] = Seq()

            /*find best cluster: */
            bisectingKMeansResult.foreach(cluster => {

              val clusterNr = cluster._1
              val clusterTools: Seq[String] = cluster._2.split(splitter).toSeq

              val train = FormatUtil.prepareDataToLabeledPoints(session, trainDF, clusterTools)

              val Array(train1, _) = train.randomSplit(Array(0.7, 0.3), seed = 123L)
              val Array(_, train2) = train.randomSplit(Array(0.3, 0.7), seed = 23L)
              val Array(train3, _) = train.randomSplit(Array(0.7, 0.3), seed = 593L)
              val Array(_, train4) = train.randomSplit(Array(0.3, 0.7), seed = 941L)
              val Array(train5, _) = train.randomSplit(Array(0.7, 0.3), seed = 3L)
              val Array(_, train6) = train.randomSplit(Array(0.3, 0.7), seed = 623L)

              val trainSamples = Seq(train1, train2, train3, train4, train5, train6)

              val Array(model1, model2, model3, model4, model5, model6) = getDecisionTreeModels(trainSamples, clusterTools).toArray

              var labeledPointsDF: DataFrame = prepareDataToLabelWithFeatures(session, testDF, clusterTools)

              val bagging1 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model1.predict(features) }
              val bagging2 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model2.predict(features) }
              val bagging3 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model3.predict(features) }
              val bagging4 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model4.predict(features) }
              val bagging5 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model5.predict(features) }
              val bagging6 = udf { (features: org.apache.spark.mllib.linalg.Vector) => model6.predict(features) }

              val featuresCol = "features"
              labeledPointsDF = labeledPointsDF
                .withColumn(s"model-1", bagging1(labeledPointsDF(featuresCol)))
                .withColumn(s"model-2", bagging2(labeledPointsDF(featuresCol)))
                .withColumn(s"model-3", bagging3(labeledPointsDF(featuresCol)))
                .withColumn(s"model-4", bagging4(labeledPointsDF(featuresCol)))
                .withColumn(s"model-5", bagging5(labeledPointsDF(featuresCol)))
                .withColumn(s"model-6", bagging6(labeledPointsDF(featuresCol)))

              // Majority wins
              val majorityVoter = udf { (tools: mutable.WrappedArray[Double]) => {
                val total = tools.length
                val sum1 = tools.count(_ == 1.0)
                val sum0 = total - sum1
                val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
                errorDecision
              }
              }

              val clusterCol = s"cluster-$clusterNr"
              val baggingColumns = (1 to trainSamples.size).map(id => labeledPointsDF(s"model-${id}"))
              val voter = labeledPointsDF
                .withColumn(clusterCol, majorityVoter(array(baggingColumns: _*)))

              //eval cluster: start
              val predictionAndLabel = FormatUtil.getPredictionAndLabel(voter, clusterCol)
              val evalCluster = F1.evalPredictionAndLabels(predictionAndLabel)
              evalCluster.printResult(s"$clusterCol eval")
              //eval cluster: finish

              if (evalCluster.f1 >= maxF1) {
                maxF1 = evalCluster.f1
                bestTools = clusterTools
              }

            })

            println(s"best tools: ${bestTools.map(getName(_)).mkString("+")} with F1: $maxF1")
            //FINISHED: clustering and select best tools.


            /*TODO: Duplication from the class: de.experiments.models.combinator.ModelsCombiner*/
            //START ensemble learning on best tools:
            val trainLabeledPointsTools = FormatUtil
              .prepareDataToLabeledPoints(session, trainDF, bestTools)

            val bayesModel = NaiveBayes.train(trainLabeledPointsTools, lambda = 1.0, modelType = "bernoulli")
            val (bestLogRegrData, bestLogRegModel) = getBestModel(maxPrecision, maxRecall, trainLabeledPointsTools, trainLabeledPointsTools)

            //start: decision tree
            val numClasses = 2
            val maxDepth = bestTools.length
            /*8*/
            val categoricalFeaturesInfo: Map[Int, Int] = (0 until maxDepth).map(attr => attr -> numClasses).toMap
            //Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2, 6 -> 2, 7 -> 2)
            //        val impurity = "entropy"
            val impurity = "gini"
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

            val unionall = "unionAll"
            val features = "features"
            val rowId = "row-id"
            val minKCol = "minK"

            val testLabeledPointsTools: DataFrame = FormatUtil
              .prepareDataToLabeledPoints(session, testDF, bestTools)
              .toDF(FullResult.label, features)
              .withColumn(rowId, monotonically_increasing_id())

            val naiveBayesCol = "naive-bayes"
            val dtCol = "decision-tree"
            val logRegrCol = "log-regr"
            val predictCol = "prediction"


            val toolsNumber = bestTools.length
            var allClassifiers = testLabeledPointsTools
              .withColumn(dtCol, predictByDT(testLabeledPointsTools(features)))
              .withColumn(naiveBayesCol, predictByBayes(testLabeledPointsTools(features)))
              .withColumn(logRegrCol, predictByLogRegr(testLabeledPointsTools(features)))
              .withColumn(unionall, minKTools(lit(1), testLabeledPointsTools(features)))
              .withColumn(minKCol, minKTools(lit(toolsNumber), testLabeledPointsTools(features)))
              .select(rowId, FullResult.label, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol)
              .toDF()

            //start:neural networks

            val nextLayer = toolsNumber + 1
            val layers = Array[Int](toolsNumber, nextLayer, toolsNumber, numClasses)
            val trainer = new MultilayerPerceptronClassifier()
              .setLayers(layers)
              .setBlockSize(128)
              .setSeed(1234L)
              .setMaxIter(100)
            val nnTrain = FormatUtil.prepareDataToLIBSVM(session, trainDF, bestTools)
            val networkModel = trainer.fit(nnTrain)
            val testWithRowId = testDF.withColumn(rowId, monotonically_increasing_id())
            val nnTest = FormatUtil.prepareDataWithRowIdToLIBSVM(session, testWithRowId, bestTools)
            val result = networkModel.transform(nnTest)
            val nnPrediction = result.select(rowId, predictCol)
            //end:neural networks

            val nNetworksCol = "n-networks"
            allClassifiers = allClassifiers
              .join(nnPrediction, rowId)
              .withColumnRenamed(predictCol, nNetworksCol)
              .select(rowId, FullResult.label, nNetworksCol, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol)


            //all possible combinations of errors classification and their counts
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
                val mink = row.getDouble(7)
                (label, nn, dt, bayes, logregr, unionAll, mink)
              })

            val countByValue = allResults.countByValue()

            println(s"label, nn, dt, naive bayes, log regression, unionall, min-$toolsNumber : count")
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
            println(s"META: COMBINE CLASSIFIERS on best cluster")

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
                  .select(FullResult.label, finalCombiCol)
                  .toDF()

              val predictionAndLabel = finalResult.rdd.map(row => {
                val label = row.getDouble(0)
                val prediction = row.getDouble(1)
                (prediction, label)
              })

              val eval = F1.evalPredictionAndLabels(predictionAndLabel)
              eval.printResult(s"min-$k combination of nn, dt, nb, logreg, unionall, min-$toolsNumber:")
            })

            //todo: Majority wins
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
            majorityEval.printResult("majority voters")
            //end: Majority wins

            val allClassifiersCols = Array(nNetworksCol, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol).toSeq

            val labeledPointsClassifiers = FormatUtil.prepareDoublesToLabeledPoints(session, allClassifiers, allClassifiersCols)

            val Array(trainClassifiers, testClassifiers) = labeledPointsClassifiers.randomSplit(Array(0.2, 0.8))

            //Logistic Regression for classifier combination:
            val (classiBestModelData, classiBestModel) =
            //todo: Achtung: we removed the max precision and max recall threshold
              ModelUtil.getBestModel(0.99, 0.99, trainClassifiers, trainClassifiers)

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
            // val impurityFinal = "entropy"
            val mDepth = 6

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


  private def getBestModel(maxPrecision: Double,
                           maxRecall: Double,
                           train: RDD[LabeledPoint],
                           test: RDD[LabeledPoint]): (ModelData, LogisticRegressionModel) = {

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


  //todo: after the POC is ready migrate this conversion to FormatUtil class.
  def prepareDataToIdWithFeatures(session: SparkSession,
                                  dataDF: DataFrame,
                                  tools: Seq[String]): DataFrame = {

    val labelAndToolsCols = Seq(FullResult.label) ++ tools
    val labelAndTools = dataDF.select(rowIdCol, labelAndToolsCols: _*)

    import session.implicits._
    val data: DataFrame = labelAndTools.map(row => {
      val id: Long = row.getLong(0)
      val label: Double = row.get(1).toString.toDouble
      val toolsVals: Array[Double] = (2 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features: org.apache.spark.mllib.linalg.Vector = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      (id, label, features)
    }).rdd.toDF(rowIdCol, FullResult.label, "features")

    data
  }

  def prepareDataToLabelWithFeatures(session: SparkSession,
                                     dataDF: DataFrame,
                                     tools: Seq[String]): DataFrame = {

    val labelAndTools = dataDF.select(FullResult.label, tools: _*)

    import session.implicits._
    val data: DataFrame = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features: org.apache.spark.mllib.linalg.Vector = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      (label, features)
    }).rdd.toDF(FullResult.label, "features")

    data
  }

  def prepareClustersToIdWithFeatures(session: SparkSession,
                                      dataDF: DataFrame,
                                      tools: Seq[String]): DataFrame = {

    val labelAndToolsCols = Seq(FullResult.label) ++ tools
    val labelAndTools = dataDF.select(rowIdCol, labelAndToolsCols: _*)

    import session.implicits._
    val data: DataFrame = labelAndTools.map(row => {
      val id: Long = row.getLong(0)
      val label: Double = row.get(1).toString.toDouble
      val toolsVals: Array[Double] = (2 until row.size)
        .map(idx => row.getDouble(idx)).toArray
      val features: org.apache.spark.mllib.linalg.Vector = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      (id, label, features)
    }).rdd.toDF(rowIdCol, FullResult.label, "features")

    data
  }

  private def evaluateCluster(session: SparkSession, dataSetName: String, trainDF: DataFrame, cluster: (Int, String))

  = {

    val nr = cluster._1
    val tools: Seq[String] = cluster._2.split(splitter).toSeq

    val toolsToEval: DataFrame = trainDF.select(FullResult.label, tools: _*)

    val linearCombi = F1.evaluateLinearCombiWithLBFGS(session, dataSetName, tools)

    val bayesCombi = F1.evaluateLinearCombiWithNaiveBayes(session, dataSetName, tools)

    val unionAll: Eval = F1.evaluate(toolsToEval)

    val num = tools.size
    //  unionAll.printResult("Union All: ")
    val minK: Eval = F1.evaluate(toolsToEval, num)
    //  minK.printResult(s"min-$num")

    val toolsRealNames: Seq[String] = tools.map(getExtName(_))

    val latexBruteForceRow =
      s"""
         |\\multirow{4}{*}{\\begin{tabular}[c]{@{}l@{}}${toolsRealNames.mkString("+")}\\\\ $$ ${linearCombi.info} $$ \\end{tabular}}
         |                                                                        & UnionAll & ${unionAll.precision} & ${unionAll.recall} & ${unionAll.f1}  \\\\
         |                                                                        & Min-$num    & ${minK.precision} & ${minK.recall} & ${minK.f1}  \\\\
         |                                                                        & LinComb  & ${linearCombi.precision} & ${linearCombi.recall} & ${linearCombi.f1} \\\\
         |                                                                        & NaiveBayes  & ${bayesCombi.precision} & ${bayesCombi.recall} & ${bayesCombi.f1} \\\\
         |\\midrule
         |
                 """.stripMargin

    latexBruteForceRow
  }
}
