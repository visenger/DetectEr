package de.playground

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.ml.clustering.{BisectingKMeans, GaussianMixture, KMeans}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 10/03/17.
  */
class ClusteringPlayground {

}

object ClusteringPlaygroundKMeans {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("K-MEANS") {
      session => {
        // Loads data.
        val dataset: DataFrame = session.read.format("libsvm").load("src/main/resources/k-means.txt")


        // Trains a k-means model.
        val kmeans: KMeans = new KMeans().setK(3).setSeed(1L)
        val model = kmeans.fit(dataset)

        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        val WSSSE = model.computeCost(dataset)
        println(s"Within Set Sum of Squared Errors = $WSSSE")

        // Shows the result.
        println("Cluster Centers: ")
        model.clusterCenters.foreach(println)

        val clusters = model.transform(dataset)
        clusters.select("label", "prediction").where(clusters("prediction") === 1).show()

      }
    }
  }
}

object ClusteringPlaygroundBisectingClustering {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("BISECTING") {
      session => {
        val dataset = session.read.format("libsvm").load("src/main/resources/k-means.txt")
        dataset.show()

        // Trains a bisecting k-means model.
        val bkm: BisectingKMeans = new BisectingKMeans().setK(3).setSeed(1)
        val model = bkm.fit(dataset)

        // Evaluate clustering.
        val cost = model.computeCost(dataset)
        println(s"Within Set Sum of Squared Errors = $cost")

        // Shows the result.
        println("Cluster Centers: ")
        val centers = model.clusterCenters
        centers.foreach(println)
      }
    }
  }
}


object ClusteringPlaygroundDQTools {
  val experimentsConf: Config = ConfigFactory.load("experiments.conf")

  val blackoakTrainFile: String = experimentsConf.getString("blackoak.experiments.train.file")
  val hospTrainFile: String = experimentsConf.getString("hosp.experiments.train.file")
  val salariesTrainFile: String = experimentsConf.getString("salaries.experiments.train.file")

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("TOOLS-CLUSTERING") {
      session => {
        import org.apache.spark.sql.functions._
        val fullDF = DataSetCreator.createFrame(session, blackoakTrainFile, FullResult.schema: _*)
        val withIdx = fullDF.withColumn("idx", monotonically_increasing_id())
        withIdx.show()
        withIdx.printSchema()

        val idxTools = withIdx.select("idx", FullResult.tools: _*)


        import session.implicits._
        val columns = (FullResult.tools).map(tool => (tool, fullDF.col(tool)))

        val transposeMatrix: DataFrame = columns.map(column => {
          val colName = column._1
          val tool = fullDF.select(column._2)
          val toolsVals: Array[Double] = tool.rdd.map(element => element.getString(0).toDouble).collect()
          val valsVector: Vector = Vectors.dense(toolsVals)
          (colName, valsVector)
        }).toDF("toolName", "features")

        transposeMatrix.show()

        val indexer = new StringIndexer()
        indexer.setInputCol("toolName")
        indexer.setOutputCol("label")

        val matrixWithIndx = indexer.fit(transposeMatrix).transform(transposeMatrix)
        matrixWithIndx.show()

        val kMeans = new KMeans()
        kMeans.setK(3)
        kMeans.setSeed(4L)
        kMeans.setMaxIter(200)

        val kMeansModel = kMeans.fit(matrixWithIndx)

        val clustering = kMeansModel.transform(matrixWithIndx)

        clustering.select("toolName", "label", "prediction").show()


        val bisectingKMeans = new BisectingKMeans()
        bisectingKMeans.setK(3)
        bisectingKMeans.setSeed(2L)
        val bisectingKMeansModel = bisectingKMeans.fit(matrixWithIndx)
        val bisectClustering = bisectingKMeansModel.transform(matrixWithIndx)
        bisectClustering.select("toolName", "label", "prediction").show()



      }
    }
  }
}


