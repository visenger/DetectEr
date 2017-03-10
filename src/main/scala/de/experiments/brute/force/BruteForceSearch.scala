package de.experiments.brute.force

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.logistic.regression.{LogisticRegressionCommonBase, TrainData}
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 27/02/17.
  */
class BruteForceSearch {

}

object BruteForceSearchRunner extends ExperimentsCommonConfig with LogisticRegressionCommonBase {
  def main(args: Array[String]): Unit = {
    val allTools = FullResult.tools

    val toolsNumber = allTools.size

    val linearCombiSchema: Array[String] = linearCombiHeader.split(sep)

    SparkLOAN.withSparkSession("BRUTE-FORCE") {
      session => {

        process_test_data {
          data => {

            val dataSetName = data._1
            println(s" DATASET: ${dataSetName}")
            val pathToData = data._2
            val fullDF: DataFrame = DataSetCreator.createFrame(session, pathToData, FullResult.schema: _*)

            (2 to toolsNumber).foreach(num => {
              val combiOfK: List[Seq[String]] = allTools.combinations(num).toList
              combiOfK.foreach(tools => {

                val toolsToEval: DataFrame = fullDF.select(FullResult.label, tools: _*)

                val linearCombi = F1.evaluateLinearCombi(session, dataSetName, tools)

                val unionAll: Eval = F1.evaluate(toolsToEval)
                //  unionAll.printResult("Union All: ")
                val minK: Eval = F1.evaluate(toolsToEval, num)
                //  minK.printResult(s"min-$num")

                val latexBruteForceRow =
                  s"""
                     |\\multirow{3}{*}{\\begin{tabular}[c]{@{}l@{}}${tools.mkString("+")}\\\\ $$ ${linearCombi.info} $$ \\end{tabular}}
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
