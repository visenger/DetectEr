package de.experiments.clustering

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import org.apache.spark.ml.clustering.{BisectingKMeans, KMeans}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 13/03/17.
  */
object ToolsClustering extends ExperimentsCommonConfig {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("TOOLS-CLUSTERING") {
      session => {

        process_train_data {
          trainFile => {

            import session.implicits._

            val dataSetName = trainFile._1
            val trainFilePath = trainFile._2

            println(s"processing: $dataSetName")

            val fullDF = DataSetCreator.createFrame(session, trainFilePath, FullResult.schema: _*)

            val columns = (FullResult.tools).map(tool => (tool, fullDF.col(tool)))

            val transposeMatrix: DataFrame = columns.map(column => {
              val colName = column._1
              val tool = fullDF.select(column._2)
              val toolsVals: Array[Double] = tool.rdd.map(element => element.getString(0).toDouble).collect()
              val valsVector: Vector = Vectors.dense(toolsVals)
              (colName, valsVector)
            }).toDF("toolName", "features")

            // transposeMatrix.show()

            val indexer = new StringIndexer()
            indexer.setInputCol("toolName")
            indexer.setOutputCol("label")

            val matrixWithIndx = indexer.fit(transposeMatrix).transform(transposeMatrix)
            //matrixWithIndx.show()

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
  }

}
