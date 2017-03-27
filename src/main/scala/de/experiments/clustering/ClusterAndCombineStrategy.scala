package de.experiments.clustering

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.logistic.regression.ModelData
import de.model.util.FormatUtil
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


/**
  * Created by visenger on 24/03/17.
  */
class ClusterAndCombineStrategy {

}

object TruthMatrixClusterAndCombineStrategy extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    run()
  }

  val rowIdCol = "row-id"

  def run(): Unit = {

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

        val k = 5
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


        /*
        *  val kMeans = new KMeans().setK(k)
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
        * */


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
      val predictionAndLabels: RDD[(Double, Double)] = train.map { case LabeledPoint(label, features) =>
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
      (id, features)
    }).rdd.toDF(rowIdCol, "features")

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
