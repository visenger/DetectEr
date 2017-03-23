package de.evaluation.data.truth.matrix

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.cosine.similarity.{AllToolsSimilarity, Cosine}
import de.model.kappa.{Kappa, KappaEstimator}
import de.model.mutual.information.{PMIEstimator, ToolPMI}
import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by visenger on 21/03/17.
  */
class TruthMatrix {

}

object TruthMatrixClusteringRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("WAHRHEITSMATRIX") {
      session => {

        println(s"CLUSTERING ON TRUTH MATRIX")

        import org.apache.spark.sql.functions._
        import session.implicits._

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)

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

        val kMeans = new KMeans()
          .setMaxIter(200)
          .setK(3)
          .setSeed(5L)


        val kMeansModel = kMeans.fit(truthMatrixWithIndx)
        val kMeansClusters: DataFrame = kMeansModel
          .transform(truthMatrixWithIndx)
          .withColumn("tool", getRealName(truthMatrixWithIndx("tool-name"))).toDF()

        val kMeansResult = kMeansClusters.select("prediction", "tool").groupByKey(row => {
          row.getInt(0)
        }).mapGroups((num, row) => {
          val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
          (num, clusterTools.mkString(", "))
        }).rdd.collect().toSeq

        println(s"kMeans")
        kMeansResult.foreach(println)

        //hierarchical clustering
        val bisectingKMeans = new BisectingKMeans()
          .setSeed(5L)
          .setK(3)
        val bisectingKMeansModel = bisectingKMeans.fit(truthMatrixWithIndx)
        val bisectingKMeansClusters = bisectingKMeansModel
          .transform(truthMatrixWithIndx)
          .withColumn("tool", getRealName(truthMatrixWithIndx("tool-name")))


        val bisectingKMeansResult: Seq[(Int, String)] = bisectingKMeansClusters.select("prediction", "tool").groupByKey(row => {
          row.getInt(0)
        }).mapGroups((num, row) => {
          val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
          (num, clusterTools.mkString(", "))
        }).rdd.collect().toSeq

        println(s"bisecting kMeans")
        bisectingKMeansResult.foreach(println)

      }
    }
  }
}

object ErrorMatrixClusteringRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("ERRORMATRIX-CLUSTERING") {
      session => {

        println(s"CLUSTERING ON ERROR MATRIX")

        import org.apache.spark.sql.functions._
        import session.implicits._

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)

        val sum = udf { features: Vector => s"${features.numNonzeros} / ${features.size} " }
        val getRealName = udf { alias: String => getExtName(alias) }

        val tools = FullResult.tools

        /*TRANSPOSE MATRIX*/
        // val columns: Seq[(String, Column)] = tools.map(t => (t, trainDF(t)))

        val transposedDF: DataFrame = tools.map(columnName => {
          //val columnName = tool
          val columnForTool = trainDF.select(columnName)
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

        val kMeans = new KMeans()
          .setMaxIter(200)
          .setK(3)
          .setSeed(5L)


        val kMeansModel = kMeans.fit(truthMatrixWithIndx)
        val kMeansClusters: DataFrame = kMeansModel
          .transform(truthMatrixWithIndx)
          .withColumn("tool", getRealName(truthMatrixWithIndx("tool-name"))).toDF()

        val kMeansResult = kMeansClusters.select("prediction", "tool").groupByKey(row => {
          row.getInt(0)
        }).mapGroups((num, row) => {
          val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
          (num, clusterTools.mkString(", "))
        }).rdd.collect().toSeq

        println(s"kMeans")
        kMeansResult.foreach(println)

        //hierarchical clustering
        val bisectingKMeans = new BisectingKMeans()
          .setSeed(5L)
          .setK(3)
        val bisectingKMeansModel = bisectingKMeans.fit(truthMatrixWithIndx)
        val bisectingKMeansClusters = bisectingKMeansModel
          .transform(truthMatrixWithIndx)
          .withColumn("tool", getRealName(truthMatrixWithIndx("tool-name")))


        val bisectingKMeansResult: Seq[(Int, String)] = bisectingKMeansClusters.select("prediction", "tool").groupByKey(row => {
          row.getInt(0)
        }).mapGroups((num, row) => {
          val clusterTools: Seq[String] = row.map(_.getString(1)).toSeq
          (num, clusterTools.mkString(", "))
        }).rdd.collect().toSeq

        println(s"bisecting kMeans")
        bisectingKMeansResult.foreach(println)

      }
    }
  }
}

object SimilaritiesOnTruthMatrixRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("WAHRHEITSMATRIX2") {
      session => {

        import org.apache.spark.sql.functions._

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)

        val nxor = udf { (label: String, tool: String) => if (label.equals(tool)) "1" else "0" }

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

        val labelAndTools = Seq(FullResult.label) ++ tools

        val allMetrics: List[(ToolPMI, Kappa, Cosine)] = labelAndTools
          .combinations(2)
          .map(pair => {

            val tool1 = pair(0)
            val tool2 = pair(1)

            val pmi: ToolPMI = new PMIEstimator().computePMI(labelAndTruth, Seq(tool1, tool2))
            val kappa: Kappa = new KappaEstimator().computeKappa(labelAndTruth, Seq(tool1, tool2))
            val cosine: Cosine = new AllToolsSimilarity().computeCosine(session, labelAndTruth, (tool1, tool2))

            (pmi, kappa, cosine)
          }).toList

        println(s"ALL METRICS PERFORMED ON TRUTH MATRIX")
        allMetrics
          .sortBy(trio => trio._1.pmi)
          //.sortBy(trio => trio._3.similarity)
          .reverse
          .foreach(trio => {

            val pmi = trio._1
            val kappa = trio._2
            val cosine = trio._3

            val tool1 = pmi.tool1
            val tool2 = pmi.tool2

            println(s"${getExtName(tool1)} _ ${getExtName(tool2)} PMI: ${pmi.pmi}, KAPPA: ${kappa.kappa}, COS: ${cosine.similarity}")
          })

      }
    }
  }
}

object SimilaritiesOnErrorMatrixRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("ERRORMATRIX") {
      session => {

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        val tools = Seq(FullResult.label) ++ FullResult.tools

        val allMetrics: List[(ToolPMI, Kappa, Cosine)] = tools.combinations(2).map(pair => {

          val tool1 = pair(0)
          val tool2 = pair(1)

          val pmi: ToolPMI = new PMIEstimator().computePMI(trainDF, Seq(tool1, tool2))
          val kappa: Kappa = new KappaEstimator().computeKappa(trainDF, Seq(tool1, tool2))
          val cosine: Cosine = new AllToolsSimilarity().computeCosine(session, trainDF, (tool1, tool2))

          (pmi, kappa, cosine)
        }).toList

        println(s"ALL METRICS PERFORMED ON ERROR MATRIX")
        allMetrics
          //          .sortBy(trio => trio._1.pmi)
          .sortBy(trio => trio._3.similarity)
          .reverse
          .foreach(trio => {

            val pmi = trio._1
            val kappa = trio._2
            val cosine = trio._3

            val tool1 = pmi.tool1
            val tool2 = pmi.tool2

            println(s"${getExtName(tool1)} _ ${getExtName(tool2)} PMI: ${pmi.pmi}, KAPPA: ${kappa.kappa}, COS: ${cosine.similarity}")
          })

      }
    }
  }
}
