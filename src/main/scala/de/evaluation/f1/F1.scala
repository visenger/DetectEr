package de.evaluation.f1


import de.model.logistic.regression.{LinearFunction, TestData}
import de.model.naive.bayes.BernoulliNaiveBayes
import de.model.util.NumbersUtil
import de.model.util.NumbersUtil.round
import org.apache.spark.partial.BoundedDouble
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.Map

/**
  * Created by visenger on 07/12/16.
  */
object F1 {

  def evaluateLinearCombiWithNaiveBayes(session: SparkSession, datasetName: String, activatedTools: Seq[String] = Seq()): Eval = {
    val bernoulliNaiveBayes = new BernoulliNaiveBayes()
    bernoulliNaiveBayes.onDatasetName(datasetName)
    bernoulliNaiveBayes.onTools(activatedTools)
    val eval: Eval = bernoulliNaiveBayes.run(session)
    eval
  }

  def evaluateLinearCombi(session: SparkSession, datasetName: String, activatedTools: Seq[String] = Seq()): Eval = {

    val linearFunction = new LinearFunction()
    linearFunction.onDatasetName(datasetName)
    linearFunction.onTools(activatedTools)
    val eval: Eval = linearFunction.evaluateLinearCombi(session)
    eval
  }

  def evaluateLinearCombiWithLBFGS(session: SparkSession, datasetName: String, activatedTools: Seq[String] = Seq()): Eval = {

    val linearFunction = new LinearFunction()
    linearFunction.onDatasetName(datasetName)
    linearFunction.onTools(activatedTools)
    val eval: Eval = linearFunction.evaluateLinearCombiWithLBFGS(session)
    eval
  }


  def evalPredictionAndLabels_TMP(predictionAndLabels: DataFrame): Eval = {
    val predictionCol = "prediction"
    val labelCol = "label"

    val counts: DataFrame = predictionAndLabels
      .groupBy(predictionAndLabels(predictionCol), predictionAndLabels(labelCol))
      .count().as("count")
      .toDF()
      .cache()

    counts.show()


    val tpDF: Dataset[Row] = counts.select("count").where(counts(predictionCol) === 1.0 && counts(labelCol) === 1.0)
    val tp = if (containsElements(tpDF)) getHeadNumber(tpDF) else 0.0
    val fnDF: Dataset[Row] = counts.select("count").where(counts(predictionCol) === 0.0 && counts(labelCol) === 1.0)
    val fn = if (containsElements(fnDF)) getHeadNumber(fnDF) else 0.0
    val tnDF: Dataset[Row] = counts.select("count").where(counts(predictionCol) === 0.0 && counts(labelCol) === 0.0)
    val tn = if (containsElements(tnDF)) getHeadNumber(tnDF) else 0.0
    val fpDF: Dataset[Row] = counts.select("count").where(counts(predictionCol) === 1.0 && counts(labelCol) === 0.0)
    val fp = if (containsElements(fpDF)) getHeadNumber(fpDF) else 0.0


    val totalData = predictionAndLabels.count()

    val accuracy = (tp + tn) / totalData.toDouble
    //    println(s"Accuracy: $accuracy")
    val precision = tp / (tp + fp).toDouble
    //println(s"Precision: $precision")

    val recall = tp / (tp + fn).toDouble
    //    println(s"Recall: $recall")

    val F1 = 2 * precision * recall / (precision + recall)
    //    println(s"F-1 Score: $F1")

    val wrongPredictions: Double = getHeadNumber(counts.select("count").where(counts(predictionCol) =!= counts(labelCol)))

    val testData = TestData(totalData, wrongPredictions.toLong, round(accuracy, 4),
      round(precision, 4), round(recall, 4), round(F1, 4), s"accuracy: ${round(accuracy, 4)}")
    Eval(testData.precision, testData.recall, testData.f1, testData.info)
  }

  private def getHeadNumber(df: Dataset[Row]): Double = {
    df.head().getLong(0).toDouble
  }

  private def containsElements(df: Dataset[Row]): Boolean = {
    df.count() > 0
  }

  def evalPredictionAndLabels(predictionAndLabels: RDD[(Double, Double)]): Eval = {

    println("started .countByValueApprox")
    val outcome: Map[(Double, Double), BoundedDouble] = predictionAndLabels.countByValueApprox(500, 0.85).getFinalValue()
    outcome.foreach(println)
    outcome.map(element => element._2.mean)
    println("finished .countByValueApprox")

    val outcomeCounts: Map[(Double, Double), Long] = predictionAndLabels.countByValue()
    outcomeCounts.foreach(println)
    println("finished .countByValue()")

    var tp = 0.0
    var fn = 0.0
    var tn = 0.0
    var fp = 0.0

    outcomeCounts.foreach(elem => {
      val values = elem._1
      val count = elem._2
      values match {
        //confusion matrix
        //(prediction, label)
        case (1.0, 1.0) => tp = count
        case (1.0, 0.0) => fp = count
        case (0.0, 1.0) => fn = count
        case (0.0, 0.0) => tn = count
      }
    })

    //    println(s"true positives: $tp")

    val totalData = predictionAndLabels.count()

    val accuracy = (tp + tn) / totalData.toDouble
    //    println(s"Accuracy: $accuracy")
    val precision = tp / (tp + fp).toDouble
    println(s"Precision: $precision")

    val recall = tp / (tp + fn).toDouble
    //    println(s"Recall: $recall")

    val F1 = 2 * precision * recall / (precision + recall)
    //    println(s"F-1 Score: $F1")

    val wrongPredictions: Double = outcomeCounts
      .count(key => key._1 != key._2)

    val testData = TestData(totalData, wrongPredictions.toLong, round(accuracy, 4),
      round(precision, 4), round(recall, 4), round(F1, 4), s"accuracy: ${round(accuracy, 4)}")
    Eval(testData.precision, testData.recall, testData.f1, testData.info)
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

    val toolAndLabel = resultDF.select(tool, FullResult.label)

    //predictionAndLabels: RDD[(Double, Double)]
    //predict->value by tool
    //label -> real error

    val predictAndLabel: RDD[(Double, Double)] = toolAndLabel
      .rdd
      .map(row => (row.getString(0).toDouble, row.getString(1).toDouble))

    val eval = evalPredictionAndLabels(predictAndLabel)
    eval
  }

  @Deprecated
  def _getEvalForTool(resultDF: DataFrame, tool: String): Eval = {
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
