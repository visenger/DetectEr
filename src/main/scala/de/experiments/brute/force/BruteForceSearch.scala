package de.experiments.brute.force

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 27/02/17.
  */
class BruteForceSearch {

}

object BruteForceSearchRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    val allTools = FullResult.tools

    val toolsNumber = allTools.size

    SparkLOAN.withSparkSession("BRUTE-FORCE") {
      session => {
        process_data {
          data => {
            println(s" DATASET: $data")
            val fullDF: DataFrame = DataSetCreator.createFrame(session, data, FullResult.schema: _*)
            (1 to toolsNumber).foreach(num => {
              val combiOfK: List[Seq[String]] = allTools.combinations(num).toList
              combiOfK.foreach(tools => {
                //println(tools.mkString(","))
                val toolsToEval: DataFrame = fullDF.select(FullResult.label, tools: _*)
                val unionAll: Eval = F1.evaluate(toolsToEval)
                val minK: Eval = F1.evaluate(toolsToEval, num)
                //println(s"""${tools.mkString("+")} & UNION-ALL: ${unionAll.toString} & MIN-$num : ${minK.toString}""")

                val latexTableRow =
                  s"""
                    \\multirow{2}{*}{${tools.mkString("+")}} & union all  & ${unionAll.precision}  & ${unionAll.recall} & ${unionAll.f1}  \\\\
                                             & min-$num   & ${minK.precision}      & ${minK.recall}     & ${minK.f1}   \\\\
                    """.stripMargin
                println(latexTableRow)
              })
            })
          }
        }

      }
    }


  }

}
