package de.experiments.features.prediction

import de.evaluation.f1.FullResult
import de.model.util.NumbersUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


class FeaturesPredictivity {

}


case class CountsTableRow(x: Double, y: Double, total: Double, xyCount: Double, xCount: Double, yCount: Double)

case class MutualInformation(column1: String, column2: String, mi: Double) {
  override def toString = {
    s"Mutual information between $column1 and $column2 = $mi"
  }
}

case class DependenceProbability(column1: String, column2: String, prob: Double) {
  override def toString = {
    s"Pairwise dependence probability between $column1 and $column2 = $prob"
  }

  def info: String = {
    "Pairwise dependence probability measures the probability that there is any relationship between columns" +
      " (how likely it is that the two columns are dependent)"
  }
}

case class Correlation(column1: String, column2: String, value: Double) {
  override def toString = s"Correlation between $column1 and $column2 is: ${NumbersUtil.round(value, 4)}"
}

case class InformationGain(column1: String, column2: String, value: Double) {
  override def toString = s"Information Gain between $column1 and $column2 is: ${NumbersUtil.round(value, 4)}"

  def info: String = {
    s"""
       |Information gain (IG) measures how much
       |“information” a feature gives us about the class.
       |– Features that perfectly partition should give maximal
       |information.
       |– Unrelated features should give no information.
    """.stripMargin
  }
}

object InformationGainUtil {

  def getInformationGain(trainSystemsAndMetaDF: DataFrame, column: String): InformationGain = {
    val labelsAndFeature: RDD[(Double, Double)] = trainSystemsAndMetaDF
      .select(FullResult.label, column)
      .rdd
      .map(row => {
        val label: Double = row.getDouble(0)
        val feature: Double = row.getDouble(1)
        (label, feature)
      })
    val countByCombinations = labelsAndFeature.countByValue()

    var tp = 0.0
    var fp = 0.0
    var fn = 0.0
    var tn = 0.0

    countByCombinations.foreach(entry => {
      entry._1 match {
        case (1.0, 1.0) => tp = entry._2
        case (0.0, 1.0) => fp = entry._2
        case (1.0, 0.0) => fn = entry._2
        case (0.0, 0.0) => tn = entry._2
      }
    })

    val pos = tp + fn
    val neg = fp + tn

    val total = pos + neg

    val entropyPosNeg: Double = computeEntropy(pos, neg)

    val prob_of_1 = (tp + fp) / total
    val prob_of_0 = 1 - prob_of_1

    val ig: Double = entropyPosNeg - (prob_of_1 * computeEntropy(tp, fp) + prob_of_0 * computeEntropy(fn, tn))


    InformationGain(FullResult.label, column, ig)


  }

  private def computeEntropy(pos: Double, neg: Double) = {
    val entropy: Double = Seq(pos, neg)
      .map(e => {
        val p_e = e / (pos + neg)
        p_e * Math.log(p_e) * (-1.0)
      })
      .filterNot(_.isNaN)
      .foldLeft(0.0)((acc, element) => acc + element)
    entropy
  }
}


