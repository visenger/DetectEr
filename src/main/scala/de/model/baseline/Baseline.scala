package de.model.baseline

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql._

import scala.collection.immutable.IndexedSeq

/**
  * Created by visenger on 08/02/17.
  */
class Baseline {

  private var fullResult = ""

  def onData(result: String): this.type = {
    fullResult = result
    this
  }

  def calculateEvalForEachTool(): Unit = {
    SparkLOAN.withSparkSession("F1FOREACHTOOL") {
      session => {

        val data: DataFrame = getData(session)

        val tools = FullResult.tools

        tools.foreach(tool => {
          val eval = F1.getEvalForTool(data, tool)
          //eval.printResult(tool)
          eval.printLatexString(tool)
        })
      }
    }
  }


  def calculateBaseline(): Unit = {
    SparkLOAN.withSparkSession("BASELINE") {
      session => {
        val data: DataFrame = getData(session)

        /* UNION ALL RESULTS: */
        val unionAll = F1.evaluate(data)
       // unionAll.printResult("UNION ALL")
        unionAll.printLatexString("UNION ALL")

        /* MIN-K RESULTS: */
        val allTools = FullResult.tools.length
        (1 to allTools).map(minK => F1.evaluate(data, minK).printLatexString(s"min-$minK"))


      }
    }
  }


  private def getData(session: SparkSession): DataFrame = {
    val blackOakFullResult = DataSetCreator.createDataSetNoHeader(session, fullResult, FullResult.schema: _*)
    blackOakFullResult
  }

}

object HospBaselineRunner {
  def main(args: Array[String]): Unit = {
    val hospBaseline = new Baseline()
    hospBaseline.onData("result.hosp.10k.full.result.file")
    hospBaseline.calculateEvalForEachTool()
    hospBaseline.calculateBaseline()
  }
}

object BlackOackBaselineRunner {

  def main(args: Array[String]): Unit = {
    val blackOakBaseline = new Baseline()
    blackOakBaseline.onData("output.full.result.file")
    blackOakBaseline.calculateEvalForEachTool()
    blackOakBaseline.calculateBaseline()


  }

}
