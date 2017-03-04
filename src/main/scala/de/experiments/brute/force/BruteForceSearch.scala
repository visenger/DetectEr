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

        val linearModel = DataSetCreator.createFrame(session, linearCombiModelPath, linearCombiSchema: _*)

        process_data {
          data => {
            val dataSetName = data._1
            println(s" DATASET: ${dataSetName}")
            val pathToData = data._2
            val fullDF: DataFrame = DataSetCreator.createFrame(session, pathToData, FullResult.schema: _*)
            val linearModelOfData = linearModel.where(linearModel("dataset") === dataSetName)
            linearModelOfData.show()
            val modelRow = linearModelOfData.head()

            val modelValuesMap: Map[String, String] = modelRow
              .getValuesMap[String](linearCombiSchema.filterNot(_.equals("dataset")))

            //modelValuesMap.foreach(println)

            val modelValues: Map[String, Double] = modelValuesMap.map(e => e._1 -> e._2.toDouble)

            (2 to toolsNumber).foreach(num => {
              val combiOfK: List[Seq[String]] = allTools.combinations(num).toList
              combiOfK.foreach(tools => {

                val toolsToEval: DataFrame = fullDF.select(FullResult.label, tools: _*)

                val linearModel: Eval = F1.evaluate(toolsToEval, modelValues, tools)
                linearModel.printResult(s"combi: ${tools.mkString(sep)}")


                //                val unionAll: Eval = F1.evaluate(toolsToEval)
                //                val minK: Eval = F1.evaluate(toolsToEval, num)
                //
                //                val latexTableRow =
                //                  s"""
                //                                \\multirow{2}{*}{${tools.mkString("+")}} & union all  & ${unionAll.precision}  & ${unionAll.recall} & ${unionAll.f1}  \\\\
                //                                                         & min-$num   & ${minK.precision}      & ${minK.recall}     & ${minK.f1}   \\\\
                //                                """.stripMargin
                //                println(latexTableRow)
              })
            })
          }
        }

      }
    }


  }

}
