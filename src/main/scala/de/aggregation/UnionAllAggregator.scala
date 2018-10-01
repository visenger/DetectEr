package de.aggregation

import de.error.detection.from.metadata.UDF
import de.evaluation.f1.{Eval, F1}
import de.model.util.FormatUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col}

class UnionAllAggregator {

  val min_1_col = "min-1"

  var columnNames: Seq[String] = Seq()
  var dataFrame: DataFrame = null

  def forColumns(names: Seq[String]): this.type = {
    columnNames = names
    this
  }

  def onDataFrame(df: DataFrame): this.type = {
    dataFrame = df
    this
  }

  def aggregate(): DataFrame = {
    val allMetadataBasedClassifiers = columnNames.map(name => col(name))

    val evaluationMatrixDF: DataFrame = dataFrame
      .withColumn(min_1_col, UDF.min_1(array(allMetadataBasedClassifiers: _*)))
    evaluationMatrixDF
  }

  def evaluate(): Eval = {
    def evaluationMatrixDF = aggregate()

    val majorityVoterDF: DataFrame = FormatUtil
      .getPredictionAndLabelOnIntegers(evaluationMatrixDF, min_1_col)
    val eval_min_1: Eval = F1.evalPredictionAndLabels_TMP(majorityVoterDF)
    eval_min_1
  }


}

object UnionAllAggregator {
  def apply(): UnionAllAggregator = new UnionAllAggregator()
}
