package de.evaluation.f1


import de.model.logistic.regression.LinearFunction
import de.model.util.NumbersUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.Map

/**
  * Created by visenger on 07/12/16.
  */
object F1 {

  def evaluateLinearCombi(session: SparkSession, datasetName: String, activatedTools: Seq[String] = Seq()): Eval = {

    val linearFunction = new LinearFunction()
    linearFunction.onDatasetName(datasetName)
    linearFunction.onTools(activatedTools)
    val eval: Eval = linearFunction.evaluateLinearCombi(session)
    eval
  }


  def evaluate(resultDF: DataFrame, model: Map[String, Double], activatedTools: Seq[String]): Eval = {
    import org.apache.spark.sql.functions._
    /**
      * activatedTools are same as tools: _*
      * val resultDF: DataFrame = fullDF.select(FullResult.label, tools: _*)
      * model: Map[String, Double]
      * (exists-2,5.25)
      * (exists-3,5.59)
      * (intercept,-10.62)
      * (trainR,0.1375)
      * (exists-5,4.27)
      * (exists-1,3.51)
      * (trainF1,0.2316)
      * (threshold,0.317)
      * (trainP,0.7333)
      * (exists-4,3.25)
      **/

    val pr: DataFrame = resultDF

    val prediction = (select: Array[(String, Column)]) => {
      val function: Column = select
        .map(row => row._2.*(model.getOrElse(row._1, 0.0)))
        .reduce((c1, c2) => c1.plus(c2))
      val intercept = model.getOrElse("intercept", 0.0)
      val z = function.+(intercept).*(-1)

      val probability: Column = lit(1.0)./(lit(1.0).+(exp(z)))

      val threshold = model.getOrElse("threshold", 0.5)
      val thresholdCol: Column = lit(threshold)
      //when($"B".isNull or $"B" === "", 0).otherwise(1)
      val found: Column = when(probability > 0.5, 1).otherwise(0)

      found
    }


    val exists: Array[String] = pr.columns.filterNot(_.equals(FullResult.label))
    val select: Array[(String, Column)] = exists.map(name => (name, pr(name)))
    val withPrediction = pr.withColumn("prediction", prediction(select))


    val zippedValues: RDD[(Int, Int)] = withPrediction.select("prediction", "label").rdd.map(row => {
      (row.get(0).toString.toInt, row.get(1).toString.toInt)
    })

    val countPredictionAndLabel: Map[(Int, Int), Long] = zippedValues.countByValue()

    //println(countPredictionAndLabel.mkString("::"))

    var tp = 0L
    var fn = 0L
    var tn = 0L
    var fp = 0L

    countPredictionAndLabel.foreach(elem => {
      val values = elem._1
      val count = elem._2
      values match {
        //(prediction, label)
        case (0, 0) => tn = count
        case (1, 1) => tp = count
        case (1, 0) => fp = count
        case (0, 1) => fn = count
      }
    })

    //    println(s"true positives: $tp")

    val precision = tp == 0L match {
      case true => 0.0
      case false => tp.toDouble / (tp + fp).toDouble
    }
    val recall = tp == 0L match {
      case true => 0.0
      case false => tp.toDouble / (tp + fn).toDouble
    }

    val F1 = (precision == 0.0 || recall == 0.0) match {
      case true => 0.0
      case false => 2 * precision * recall / (precision + recall)
    }

    import NumbersUtil.round
    Eval(round(precision, 4), round(recall, 4), round(F1, 4))

  }


  def evaluate(resultDF: DataFrame, k: Int = 0): Eval = {
    val toolsReturnedErrors: Dataset[Row] = toolsAgreeOnError(resultDF, k)

    val labeledAsError: Column = resultDF.col(FullResult.label) === "1"

    val selected = toolsReturnedErrors.count()


    val tp = toolsReturnedErrors.filter(labeledAsError).count()

    val correct = resultDF.filter(labeledAsError).count()

    // val precision = tp.toDouble / selected.toDouble
    val precision = tp == 0 match {
      case true => 0.0
      case false => tp.toDouble / selected.toDouble
    }
    val recall = tp == 0 match {
      case true => 0.0
      case false => tp.toDouble / correct.toDouble
    }

    val F1 = (precision == 0.0 || recall == 0.0) match {
      case true => 0.0
      case false => 2 * precision * recall / (precision + recall)
    }
    //val F1 = 2 * precision * recall / (precision + recall)
    import NumbersUtil.round
    // Eval(precision, recall, F1)
    Eval(round(precision, 4), round(recall, 4), round(F1, 4))
  }

  //  def round(percentageFound: Double, scale: Int = 2) = {
  //    BigDecimal(percentageFound).setScale(scale, RoundingMode.HALF_UP).toDouble
  //  }

  private def toolsAgreeOnError(resultDF: DataFrame, k: Int = 0): Dataset[Row] = {
    val toolsReturnedErrors: Dataset[Row] = resultDF.filter(row => {
      val fieldNames = row.schema.fieldNames
      val rowAsMap: Map[String, String] = row.getValuesMap[String](fieldNames)
      val toolsMap: Map[String, String] = rowAsMap.partition(_._1.startsWith("exists"))._1
      val toolsIndicatedError: Int = toolsMap.values.count(_.equals("1"))

      val isUnionAll = (k == 0)
      val toolsAgreeOnError: Boolean = isUnionAll match {
        case true => toolsIndicatedError > k // the union-all case
        case false => toolsIndicatedError >= k // the min-k case
      }

      toolsAgreeOnError
    })
    toolsReturnedErrors
  }

  def getEvalForTool(resultDF: DataFrame, tool: String): Eval = {
    val labelAndTool = resultDF.select(FullResult.label, tool)
    val toolIndicatedError = labelAndTool.filter(resultDF.col(tool) === "1")

    val labeledAsError = resultDF.col(FullResult.label) === "1"

    val selected = toolIndicatedError.count()
    val correct = labelAndTool.filter(labeledAsError).count()

    val tp = toolIndicatedError.filter(labeledAsError).count()

    val precision = tp.toDouble / selected.toDouble
    val recall = tp.toDouble / correct.toDouble

    val F1 = 2 * precision * recall / (precision + recall)
    import NumbersUtil.round
    val eval = Eval(round(precision, 4), round(recall, 4), round(F1, 4))
    // val eval = Eval(precision, recall, F1)
    eval
  }


  def evaluateResult(goldStandard: DataFrame, selected: DataFrame): Eval = {
    val tpDataset: DataFrame = goldStandard.intersect(selected)
    val tp: Long = tpDataset.count()

    val fnDataset: DataFrame = goldStandard.except(tpDataset)
    val fn = fnDataset.count()

    val fpDataset: DataFrame = selected.except(tpDataset)
    val fp: Long = fpDataset.count()

    //println(s"tp= $tp, fn= $fn, fp=$fp")
    val precision = tp.toDouble / (tp + fp).toDouble
    val recall = tp.toDouble / (tp + fn).toDouble
    val F1 = (2 * precision * recall) / (precision + recall)
    Eval(precision, recall, F1)
  }


}
