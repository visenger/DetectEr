package de.aggregation

import de.error.detection.from.metadata.UDF
import de.evaluation.f1.{Eval, F1}
import de.model.util.FormatUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col}

class MajorityVotingAggregator {

  val majority_voter = "majority-vote"

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
      .withColumn(majority_voter, UDF.majority_vote(array(allMetadataBasedClassifiers: _*)))
    evaluationMatrixDF
  }

  def evaluate(): Eval = {
    def evaluationMatrixDF = aggregate()

    val majorityVoterDF: DataFrame = FormatUtil
      .getPredictionAndLabelOnIntegers(evaluationMatrixDF, majority_voter)
    val eval_majority_voter: Eval = F1.evalPredictionAndLabels_TMP(majorityVoterDF)
    eval_majority_voter
  }

}

object MajorityVotingAggregator {
  def apply(): MajorityVotingAggregator = new MajorityVotingAggregator()
}
