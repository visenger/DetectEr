package de.evaluation.f1

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

/**
  * Created by visenger on 07/12/16.
  */
object F1 {

  def evaluate(resultDF: DataFrame, k: Int = 0): Eval = {
    val toolsReturnedErrors: Dataset[Row] = toolsAgreeOnError(resultDF, k)

    val labeledAsError: Column = resultDF.col(FullResult.label) === "1"

    val selected = toolsReturnedErrors.count()

    val tp = toolsReturnedErrors.filter(labeledAsError).count()

    val correct = resultDF.filter(labeledAsError).count()

    val precision = tp.toDouble / selected.toDouble
    val recall = tp.toDouble / correct.toDouble
    val F1 = 2 * precision * recall / (precision + recall)

    Eval(precision, recall, F1)
  }

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

    val eval = Eval(precision, recall, F1)
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
