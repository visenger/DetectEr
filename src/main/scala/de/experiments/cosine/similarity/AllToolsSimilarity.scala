package de.experiments.cosine.similarity

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.NumbersUtil
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by visenger on 01/03/17.
  */
class AllToolsSimilarity {

  def computeCosine(session: SparkSession, fullDF: DataFrame, tools: Tuple2[String, String]): Cosine = {
    import session.implicits._

    val firstTool = tools._1
    val secondTool = tools._2

    val firstTwoCols = fullDF.select(firstTool, secondTool)
    val nominator: Double = firstTwoCols
      .map(row => {
        row.getString(0).toDouble * row.getString(1).toDouble
      })
      .reduce((a, b) => a + b)

    val elementsPoweredBy2: Dataset[(Double, Double)] = firstTwoCols.map(row => {
      val first: Double = Math.pow(row.getString(0).toDouble, 2)
      val second: Double = Math.pow(row.getString(1).toDouble, 2)
      (first, second)
    })
    val vectorsSizes: (Double, Double) = elementsPoweredBy2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    val denominator = Math.sqrt(vectorsSizes._1) * Math.sqrt(vectorsSizes._2)

    val cosineSimi = nominator / denominator

    Cosine(firstTool, secondTool, NumbersUtil.round(cosineSimi, 4))

  }

}

case class Cosine(tool1: String, tool2: String, similarity: Double)

object AllToolsSimilarityRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("COSINE") {
      session => {

        val toolsSimilarity = new AllToolsSimilarity()

        val fullDF: DataFrame = DataSetCreator.createFrame(session, salariesTrainFile, FullResult.schema: _*)

        val selectLabelAndTools: DataFrame = fullDF
          .select(FullResult.label, FullResult.tools: _*)
          .cache()
        //todo: ACHTUNG: I was not able to map columns back to tools
        //
        //
        //        val rows: RDD[Vector] = selectLabelAndTools.rdd.map(row => {
        //          val valuesMap: Map[String, String] = row.getValuesMap[String](Seq(FullResult.label) ++ FullResult.tools)
        //          val valsAsNumbers: Array[Double] = valuesMap.values.map(v => v.toDouble).toArray
        //
        //          Vectors.dense(valsAsNumbers)
        //
        //        }).cache()
        //
        //        val matrix: RowMatrix = new RowMatrix(rows)
        //
        //
        //        val columnSimilarities: CoordinateMatrix = matrix.columnSimilarities(0.1)
        //
        //
        //        println(s"exact similarities rows: ${columnSimilarities.numRows()}, cols: ${columnSimilarities.numCols()}")
        //
        //
        //        val labelsSimilarities = columnSimilarities.entries.filter {
        //          case MatrixEntry(i, j, v) => i == 0
        //        }
        //        labelsSimilarities.foreach {
        //          case MatrixEntry(i, j, v) => println(s"ground truth to tool-$j similarity: ${NumbersUtil.round(v, 4)}")
        //        }
        //
        //        val toolsSimis = columnSimilarities.entries.filter {
        //          case MatrixEntry(i, j, v) => i != 0
        //        }
        //        toolsSimis.foreach {
        //          case MatrixEntry(i, j, v) => println(s"cosine(tool-$i, tool-$j) = ${NumbersUtil.round(v, 4)}")
        //        }

        //        val threshold = 0.1
        //        val columnSimilaritiesWithThreshold: CoordinateMatrix = matrix.columnSimilarities(threshold)

        //        val vectors: Seq[Vector] = rows.collect().toSeq
        //        val transpose: RDD[Vector] = session.sparkContext.parallelize(vectors.transpose.flatten)
        //

        val labelAndTools: Seq[String] = Seq(FullResult.label) ++ FullResult.tools
        val allToolsCombi: List[(String, String)] = labelAndTools.combinations(2)
          .toList
          .map(tools => (tools(0), tools(1)))

        val cosines: List[Cosine] = allToolsCombi
          .map(combi => toolsSimilarity.computeCosine(session, selectLabelAndTools, combi))

        cosines.foreach(simi =>
          println(s"my_cosine(${simi.tool1}, ${simi.tool2})= ${NumbersUtil.round(simi.similarity, 4)}"))

      }
    }
  }


  //  def computeCosine(session: SparkSession, fullDF: DataFrame, tools: Tuple2[String, String]): Cosine = {
  //    import session.implicits._
  //
  //    val firstTool = tools._1
  //    val secondTool = tools._2
  //
  //    val firstTwoCols = fullDF.select(firstTool, secondTool)
  //    val nominator: Double = firstTwoCols
  //      .map(row => {
  //        row.getString(0).toDouble * row.getString(1).toDouble
  //      })
  //      .reduce((a, b) => a + b)
  //
  //    val elementsPoweredBy2: Dataset[(Double, Double)] = firstTwoCols.map(row => {
  //      val first: Double = Math.pow(row.getString(0).toDouble, 2)
  //      val second: Double = Math.pow(row.getString(1).toDouble, 2)
  //      (first, second)
  //    })
  //    val vectorsSizes: (Double, Double) = elementsPoweredBy2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))
  //
  //    val denominator = Math.sqrt(vectorsSizes._1) * Math.sqrt(vectorsSizes._2)
  //
  //    val cosineSimi = nominator / denominator
  //
  //    Cosine(firstTool, secondTool, cosineSimi)
  //
  //  }
}
