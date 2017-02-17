package de.model.logistic.regression

/**
  * Created by visenger on 16/02/17.
  */
case class TrainData(regParam: Double,
                     elasticNetParam: Double,
                     modelCoefficients: Array[Double],
                     modelIntercept: Double,
                     maxFMeasure: Double,
                     areaUnderRoc: Double) {
  override def toString: String = {

    s"TRAIN: regParam: $regParam, elasticNetParam: $elasticNetParam,  maxFMeasure: $maxFMeasure, AreaUnderRoc: $areaUnderRoc, ModelCoefficients: ${modelCoefficients.mkString(",")}, ModelIntercept: $modelIntercept"
  }

  def createModelFormula(ind: Int): String = {
    var i = 0
    val function = modelCoefficients.map(c => {
      if (c < 0) s"(${c})t_{${i += 1; i;}}" else s"${c}t_{${i += 1; i;}}"
    }).mkString(" + ")
    function
    //s"""P(err)=\\frac{1}{1+\\exp ^{-($modelIntercept+$function)}}"""
    s""" t_{$ind}=$modelIntercept+$function """
  }
}
