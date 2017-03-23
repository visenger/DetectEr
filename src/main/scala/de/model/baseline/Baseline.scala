package de.model.baseline

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql._

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
    val config = ConfigFactory.load("experiments.conf")
    SparkLOAN.withSparkSession("F1FOREACHTOOL") {
      session => {

        val data: DataFrame = getData(session)

        val tools = FullResult.tools

        tools.foreach(tool => {
          val eval = F1.getEvalForTool(data, tool)
          //eval.printResult(tool)
          eval.printLatexString(config.getString(s"dictionary.names.$tool"))
        })
      }
    }
  }

  def ext_calculateEvalForEachTool(): Unit = {
    val config = ConfigFactory.load("experiments.conf")
    SparkLOAN.withSparkSession("F1FOREACHTOOL") {
      session => {

        val data: DataFrame = getData(session)

        val tools = FullResult.tools

        tools.foreach(tool => {
          val eval = F1.getEvalForTool(data, tool)
          //eval.printResult(tool)
          eval.printLatexString(config.getString(s"ext.dictionary.names.$tool"))
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
    //    val blackOakFullResult = DataSetCreator.createDataSetNoHeader(session, fullResult, FullResult.schema: _*)
    //    blackOakFullResult
    val data = DataSetCreator.createFrame(session, fullResult, FullResult.schema: _*)
    data
  }

}

object HospBaselineRunner {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val hospBaseline = new Baseline()
    hospBaseline.onData(config.getString("result.hosp.10k.full.result.file"))
    hospBaseline.calculateEvalForEachTool()
    hospBaseline.calculateBaseline()
  }
}

object SalariesBaselineRunner {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val salariesBaseline = new Baseline()
    salariesBaseline.onData(config.getString("result.salaries.full.result.file"))
    salariesBaseline.calculateEvalForEachTool()
    salariesBaseline.calculateBaseline()
  }
}

object BlackOackBaselineRunner {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val blackOakBaseline = new Baseline()
    blackOakBaseline.onData(config.getString("output.full.result.file"))
    blackOakBaseline.calculateEvalForEachTool()
    blackOakBaseline.calculateBaseline()


  }

}

object ExtBlackoakBaselineRunner {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("experiments.conf")
    val baseline = new Baseline()
    baseline.onData(config.getString("ext.blackoak.experiments.train.file"))
    baseline.ext_calculateEvalForEachTool()
    baseline.calculateBaseline()

    SparkLOAN.withSparkSession("LINCOMBI") {
      session => {
        val data = "ext.blackoak"
        val tools = FullResult.tools
        val linearCombiEval: Eval = F1.evaluateLinearCombiWithLBFGS(session, data, tools)
        linearCombiEval.printResult("all tools lin combi: ")
        val combiWithNaiveBayes = F1.evaluateLinearCombiWithNaiveBayes(session, data, tools)
        combiWithNaiveBayes.printResult("all tools naive bayes: ")
      }
    }
  }
}
