package de.model.kappa

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.NumbersUtil
import org.apache.spark.sql.DataFrame

/**
  * Tools aggregation model based on kappa statistics:
  * http://www.statisticshowto.com/cohens-kappa-statistic/
  *
  *
  * The Kappa statistic varies from 0 to 1, where.
  * *
  * 0 = agreement equivalent to chance.
  *0.1 – 0.20 = slight agreement.
  *0.21 – 0.40 = fair agreement.
  *0.41 – 0.60 = moderate agreement.
  *0.61 – 0.80 = substantial agreement.
  *0.81 – 0.99 = near perfect agreement
  * 1 = perfect agreement.
  */

case class Kappa(tool1: String, tool2: String, kappa: Double = 0.0) {
  override def toString: String = {
    s"$tool1, $tool2, kappa: $kappa"
  }
}

class KappaEstimator {

  //paths to the files
  private var trainData = ""
  private var testData = ""

  def trainKappaOnData(train: String): this.type = {
    trainData = train
    this
  }

  def testKappaOnData(test: String): this.type = {
    testData = test
    this
  }


  def runKappa() = {
    SparkLOAN.withSparkSession("KAPPA") {
      session => {
        val trainDF: DataFrame = DataSetCreator.createFrame(session, trainData, FullResult.schema: _*)
        val kappas: List[Kappa] = trainModel(trainDF)

        kappas.foreach(println)

        //        val testDF: DataFrame = DataSetCreator.createFrame(session, testData, FullResult.schema: _*)
        //
        //        evaluateOnTestData(testDF, kappas)
      }
    }
  }

  private def evaluateOnTestData(testDF: DataFrame, kappas: List[Kappa]) = {
    //todo: finish
    kappas.foreach(println)
  }

  private def trainModel(trainDF: DataFrame): List[Kappa] = {

    val toolsCombi = FullResult.tools.combinations(2)

    val kappas: List[Kappa] = toolsCombi.map(pair => {
      val kappaForPair = computeKappa(trainDF, pair)
      kappaForPair
    }).toList

    val meaningfullKappas = kappas.filter(kappaOnTools => kappaOnTools.kappa >= 0.1)
    meaningfullKappas.sortWith((t1, t2) => t1.kappa > t2.kappa)
  }

  def computeKappa(model: DataFrame, pairOfTools: Seq[String]): Kappa = {
    require(pairOfTools.size == 2,
      message = "The computation of kappa-statistics requires pair of columns to process")
    val tool1 = pairOfTools(0)
    val tool2 = pairOfTools(1)

    val total = model.count().toDouble

    val toolsResult = model.select(tool1, tool2)

    val numberInAgreement = toolsResult.where(toolsResult(tool1) === toolsResult(tool2)).count().toDouble

    val firstRated1 = toolsResult.where(toolsResult(tool1) === "1").count().toDouble
    val secondRated1 = toolsResult.where(toolsResult(tool2) === "1").count().toDouble

    val firstRated0 = toolsResult.where(toolsResult(tool1) === "0").count().toDouble
    val secondRated0 = toolsResult.where(toolsResult(tool2) === "0").count().toDouble

    val relativeObservedAgreement: Double = numberInAgreement / total

    val probChanceAgreement: Double = (firstRated1 * secondRated1 + firstRated0 * secondRated0) / Math.pow(total, 2)

    val diff: Double = relativeObservedAgreement - probChanceAgreement
    val kappa: Double = diff / (1.0 - probChanceAgreement)

    //  println(s"""$tool1 + $tool2: total: $total; diff:$diff -> relative obs agreement: $relativeObservedAgreement; prob of chance agreement: $probChanceAgreement""")
    Kappa(tool1, tool2, NumbersUtil.round(kappa, 4))
  }

}


