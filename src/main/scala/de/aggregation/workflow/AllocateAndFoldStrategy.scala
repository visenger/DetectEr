package de.aggregation.workflow

import de.evaluation.f1.{Eval, F1, FullResult, GoldStandard}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.cosine.similarity.{AllToolsSimilarity, Cosine}
import de.model.kappa.{Kappa, KappaEstimator}
import de.model.mutual.information.{PMIEstimator, ToolPMI}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.immutable.Seq

/**
  * Tools aggregation strategy:
  * 1.Step: Resources allocation (multiarmed bandit based tools selection)
  * 2.Step: Resources folding (tools clustering based on similarity measures)
  */
class AllocateAndFoldStrategy {

}

case class Tool(name: String)

case class ToolsCombination(combi: List[Tool])

case class UnionAll(precision: Double, recall: Double, f1: Double)

case class MinK(k: Int, precision: Double, recall: Double, f1: Double)

//Kappa, //Cosine, //ToolPMI
case class AggregatedTools(dataset: String,
                           combi: ToolsCombination,
                           unionEval: UnionAll,
                           minK: MinK,
                           allPMI: List[ToolPMI],
                           allCosineSimis: List[Cosine],
                           allKappas: List[Kappa])


object AllocateAndFoldStrategyRunner extends ExperimentsCommonConfig with de.model.multiarmed.bandit.ExperimentsBase {
  def main(args: Array[String]): Unit = {


    SparkLOAN.withSparkSession("STRATEGY-FOR-TOOLS") {
      session => {
        import session.implicits._
        val experimentsCSV = DataSetCreator.createFrame(session, multiArmedBandResults, schema: _*)

        //experimentsCSV.show()
        //header: "dataset,banditalg,param,expectations"
        // implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
        val byDataset = experimentsCSV.groupByKey(row => row.getAs[String]("dataset"))

        val allData: Dataset[(String, String, String)] = byDataset.flatMapGroups((dataset, bandits) => {
          //pro dataset
          val algorithmsAndToolsCombi: Iterator[(String, String)] = bandits.flatMap(banditAlg => {
            //pro algorithm on dataset:
            val banditRow: Map[String, String] = banditAlg.getValuesMap[String](schema)
            val expectations = banditRow.getOrElse("expectations", "")
            val Array(a, b, c, d, e) = expectations.split("\\|")
            //1:0.4143 -> toolId:expectation
            val resultOfBanditRun: Seq[ToolExpectation] =
              Seq(a, b, c, d, e)
                .map(e => new ToolExpectation().apply(e))
            val sortedResults = resultOfBanditRun
              .sortWith((t1, t2) => t1.expectation > t2.expectation)

            val toolsCombiByAlgorithm: Seq[String] =
              (2 until sortedResults.size)
                .map(toolsNum => {
                  //top N tools:
                  val whatToolsToEval: Seq[ToolExpectation] = sortedResults.take(toolsNum)
                  val selectTools = whatToolsToEval.map(t => s"${GoldStandard.exists}-${t.id}").mkString(",")
                  selectTools
                })
            val algorithm = banditRow.getOrElse("banditalg", "")
            val algorithmToTools = toolsCombiByAlgorithm.map(tools => (algorithm, tools))
            algorithmToTools
          })
          val dataToAlgorithmToTools = algorithmsAndToolsCombi.map(e => {
            (dataset, e._1, e._2)
          })
          dataToAlgorithmToTools
        })

        val experimentSettings: DataFrame = allData.toDF("dataset", "banditalg", "toolscombi")
        // experimentSettings.show(100)

        //experimentSettings.rdd.collect().foreach(println)

        val allDatasets: Array[String] = experimentsCSV
          .select(experimentsCSV.col("dataset"))
          .distinct()
          .map(row => row.getString(0))
          .collect()

        //going through all datasets:
        allDatasets.foreach(data => {
          val experimentsByDataset: Dataset[Row] =
            experimentSettings
              .where(experimentSettings.col("dataset") === data)

          val allCombis: Array[String] = experimentsByDataset
            .select(experimentSettings.col("toolscombi"))
            .distinct()
            .map(row => row.getString(0))
            .collect()

          val toolsCombinations: List[ToolsCombination] = allCombis.map(l => {
            val toolsAsList: List[Tool] = l.split(",").map(Tool(_)).toList
            ToolsCombination(toolsAsList)
          }).toSet.toList


          //todo: estimate max precision and recall from the baseline:

          val path = getTestDatasetPath(data)
          val fullResult: DataFrame = DataSetCreator.createFrame(session, path, FullResult.schema: _*)

          val pmiEstimator = new PMIEstimator()
          val kappaEstimator = new KappaEstimator()
          val toolsSimilarity = new AllToolsSimilarity()


          val aggregatedTools: List[AggregatedTools] = toolsCombinations.map(toolsCombination => {
            val tools: List[String] = toolsCombination.combi.map(_.name)
            val labelAndTools = fullResult.select(FullResult.label, tools: _*).cache()
            val unionAllEval: Eval = F1.evaluate(labelAndTools)

            val unionAll = UnionAll(unionAllEval.precision, unionAllEval.recall, unionAllEval.f1)

            val k = tools.size
            val minKEval = F1.evaluate(labelAndTools, k)
            val minK = MinK(k, minKEval.precision, minKEval.recall, minKEval.f1)


            val allMetrics: List[(ToolPMI, Kappa, Cosine)] = tools.combinations(2).map(pair => {

              val tool1 = pair(0)
              val tool2 = pair(1)

              val pmi: ToolPMI = pmiEstimator.computePMI(labelAndTools, Seq(tool1, tool2))
              val kappa: Kappa = kappaEstimator.computeKappa(labelAndTools, Seq(tool1, tool2))
              val cosine: Cosine = toolsSimilarity.computeCosine(session, labelAndTools, (tool1, tool2))

              (pmi, kappa, cosine)
            }).toList


            val allPMIs: List[ToolPMI] = allMetrics.map(_._1)
            val allKappas: List[Kappa] = allMetrics.map(_._2)
            val allCosine: List[Cosine] = allMetrics.map(_._3)

            AggregatedTools(data, toolsCombination, unionAll, minK, allPMIs, allCosine, allKappas)
          })

          println(s"data: $data -> ${toolsCombinations.size} ; ")

          aggregatedTools.foreach(println)


        })

        //        allDatasets.foreach(data => {
        //          val path = getDatasetPath(data)
        //          val fullResult: DataFrame = DataSetCreator.createFrame(session, path, FullResult.schema: _*)
        //
        //          val experimentsByDataset: Dataset[Row] =
        //            experimentSettings
        //              .where(experimentSettings.col("dataset") === data)
        //
        //          val allCombis: Array[String] = experimentsByDataset
        //            .select(experimentSettings.col("toolscombi"))
        //            .distinct()
        //            .map(row => row.getString(0))
        //            .collect()
        //
        //          val toolsCombinations: List[List[String]] = allCombis.map(l => {
        //            val toolsAsList: List[String] = l.split(",").toList
        //            toolsAsList
        //          }).toSeq.toList
        //
        //          println(s"EVALUATING: $data")
        //          toolsCombinations.foreach(topTools => {
        //            //println(s"""TOOLS COMBI: ${topTools.mkString("+")}""")
        //            val label = FullResult.label
        //            val labelAndTopTools = fullResult.select(label, topTools: _*)
        //            val eval: Eval = F1.evaluate(labelAndTopTools)
        //            //eval.printResult("union all")
        //
        //
        //            val k = topTools.length
        //            val minK: Eval = F1.evaluate(labelAndTopTools, k)
        //            //minK.printResult(s"min-$k")
        //            val latexTableRow =
        //              s"""
        //                  \\multirow{2}{*}{Top-1} & \\multirow{2}{*}{${topTools.mkString("+")}} & union all  & ${eval.precision}        & ${eval.recall}     & ${eval.f1}  \\\\
        //                             &                              & min-$k      & ${minK.precision}        & ${minK.recall}     & ${minK.f1}   \\\\
        //          """.stripMargin
        //            println(latexTableRow)
        //
        //          })
        //
        //
        //        })


      }
    }


  }


}
