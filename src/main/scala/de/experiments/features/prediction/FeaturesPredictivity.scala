package de.experiments.features.prediction

import de.model.util.NumbersUtil


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


