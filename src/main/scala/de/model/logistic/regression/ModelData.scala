package de.model.logistic.regression

/**
  * Created by visenger on 23/03/17.
  */

case class ModelData(bestThreshold: Double,
                     coefficients: Array[Double],
                     intercept: Double,
                     precision: Double,
                     recall: Double,
                     f1: Double) {

  override def toString: String = {

    s""" MODEL:
       |bestThreshold: $bestThreshold,
       |precision: $precision,
       |recall: $recall,
       |F1: $f1,
       |ModelCoefficients: ${coefficients.mkString(",")},
       |ModelIntercept: $intercept""".stripMargin
  }

  def createModelFormula(ind: Int): String = {
    var i = 0
    val function = coefficients.map(c => {
      val idx = {
        i += 1;
        i;
      }
      if (c < 0) s"(${c})t_{$idx}" else s"${c}t_{$idx}"
    }).mkString(" + ")
    function
    //s"""P(err)=\\frac{1}{1+\\exp ^{-($modelIntercept+$function)}}"""
    s""" y_{$ind}=$intercept+$function """
  }


}

object ModelData {
  def emptyModel: ModelData = ModelData(0.0, Seq.empty[Double].toArray, 0.0, 0.0, 0.0, 0.0)
}

