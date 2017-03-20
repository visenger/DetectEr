package de.experiments.brute.force

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 20/03/17.
  */

trait ExternalExterimentsCommonBase {

  val experimentsConf = ConfigFactory.load("experiments.conf")

  val allTestData: Map[String, String] = Map("ext.blackoak" -> experimentsConf
    .getString("ext.blackoak.experiments.test.file"))

  def process_ext_test_data(f: Tuple2[String, String] => Unit): Unit = {
    allTestData.foreach(t => f(t))
  }

  def getName(alias: String): String = {
    experimentsConf.getString(s"ext.dictionary.names.$alias")
  }


}

class ExternalToolsBruteForceSearch {

}

object ExternalToolsBruteForceSearchRunner extends ExternalExterimentsCommonBase {
  def main(args: Array[String]): Unit = {
    val allTools = FullResult.tools

    val toolsNumber = allTools.size

    SparkLOAN.withSparkSession("EXT-BRUTE-FORCE") {
      session => {
        process_ext_test_data {
          data => {

            val dataSetName = data._1
            println(s" DATASET: ${dataSetName}")
            val pathToData = data._2
            val fullDF: DataFrame = DataSetCreator.createFrame(session, pathToData, FullResult.schema: _*)

            (2 to toolsNumber).foreach(num => {
              val combiOfK: List[Seq[String]] = allTools.combinations(num).toList
              combiOfK.foreach(tools => {

                val toolsToEval: DataFrame = fullDF.select(FullResult.label, tools: _*)

                val linearCombi = F1.evaluateLinearCombiWithLBFGS(session, dataSetName, tools)

                val unionAll: Eval = F1.evaluate(toolsToEval)
                //  unionAll.printResult("Union All: ")
                val minK: Eval = F1.evaluate(toolsToEval, num)
                //  minK.printResult(s"min-$num")

                val toolsRealNames: Seq[String] = tools.map(getName(_))

                val latexBruteForceRow =
                  s"""
                     |\\multirow{3}{*}{\\begin{tabular}[c]{@{}l@{}}${toolsRealNames.mkString("+")}\\\\ $$ ${linearCombi.info} $$ \\end{tabular}}
                     |                                                                        & UnionAll & ${unionAll.precision} & ${unionAll.recall} & ${unionAll.f1}  \\\\
                     |                                                                        & Min-$num    & ${minK.precision} & ${minK.recall} & ${minK.f1}  \\\\
                     |                                                                        & LinComb  & ${linearCombi.precision} & ${linearCombi.recall} & ${linearCombi.f1} \\\\
                     |\\midrule
                     |
                 """.stripMargin

                println(latexBruteForceRow)

              })
            })
          }
        }

      }
    }


  }
}


