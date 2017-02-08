package de.model.baseline

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql._

import scala.collection.immutable.IndexedSeq

/**
  * Created by visenger on 08/02/17.
  */
class BlackOakBaseline {

  def calculateEvalForEachTool(): Unit = {
    SparkLOAN.withSparkSession("F1FOREACHTOOL") {
      session => {

        val blackOakFull: DataFrame = getData(session)

        val tools = FullResult.tools

        tools.foreach(tool => {
          val eval = F1.getEvalForTool(blackOakFull, tool)
          eval.printResult(tool)
        })
      }
    }
  }

  //   def getEvalForTool(blackOakFull: DataFrame, tool: String): Eval = {
  //    val labelAndTool = blackOakFull.select(FullResult.label, tool)
  //    val toolIndicatedError = labelAndTool.filter(blackOakFull.col(tool) === "1")
  //
  //    val labeledAsError = blackOakFull.col(FullResult.label) === "1"
  //
  //    val selected = toolIndicatedError.count()
  //    val correct = labelAndTool.filter(labeledAsError).count()
  //
  //    val tp = toolIndicatedError.filter(labeledAsError).count()
  //
  //    val precision = tp.toDouble / selected.toDouble
  //    val recall = tp.toDouble / correct.toDouble
  //
  //    val F1 = 2 * precision * recall / (precision + recall)
  //
  //    val eval = Eval(precision, recall, F1)
  //    eval
  //  }

  def calculateBaseline(): Unit = {
    SparkLOAN.withSparkSession("BASELINE") {
      session => {
        val blackOakFull: DataFrame = getData(session)

        /* UNION ALL RESULTS: */
        val unionAll = F1.evaluate(blackOakFull)
        unionAll.printResult("BlackOak UNION ALL")

        /* MIN-K RESULTS: */
        val allTools = FullResult.tools.length
        (1 to allTools).map(minK => F1.evaluate(blackOakFull, minK).printResult(s"min-$minK"))


      }
    }
  }

  /* def evaluateResult(blackOakFull: DataFrame, k: Int = 0): Eval = {
    val toolsReturnedErrors: Dataset[Row] = toolsAgreeOnError(blackOakFull, k)

    val labeledAsError: Column = blackOakFull.col(FullResult.label) === "1"

    val selected = toolsReturnedErrors.count()

    val tp = toolsReturnedErrors.filter(labeledAsError).count()

    val correct = blackOakFull.filter(labeledAsError).count()

    val precision = tp.toDouble / selected.toDouble
    val recall = tp.toDouble / correct.toDouble
    val F1 = 2 * precision * recall / (precision + recall)

    Eval(precision, recall, F1)
  }

   def toolsAgreeOnError(blackOakFull: DataFrame, k: Int = 0): Dataset[Row] = {
    val toolsReturnedErrors: Dataset[Row] = blackOakFull.filter(row => {
      val rowAsMap: Map[String, String] = row.getValuesMap[String](FullResult.schema)
      val toolsMap: Map[String, String] = rowAsMap.partition(_._1.startsWith("exists"))._1
      val toolsIndicatedError: Int = toolsMap.values.count(_.equals("1"))

      val isUnionAll = (k == 0)
      val toolsAgreeOnError: Boolean = isUnionAll match {
        case true => toolsIndicatedError > k // the union-all case
        case false => toolsIndicatedError == k // the min-k case
      }

      toolsAgreeOnError
    })
    toolsReturnedErrors
  }
*/
  def getData(session: SparkSession): DataFrame = {
    val blackOakFullResult = DataSetCreator.createDataSetNoHeader(session, "output.full.result.file", FullResult.schema: _*)
    blackOakFullResult
  }

}

object BlackOakBaselineRunner {

  def main(args: Array[String]): Unit = {
    val blackOakBaseline = new BlackOakBaseline()
    blackOakBaseline.calculateEvalForEachTool()
    blackOakBaseline.calculateBaseline()
  }

}
