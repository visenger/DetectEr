package de.experiments.multiarmed.bandits

import com.google.common.base.Strings
import de.aggregation.workflow.{Tool, ToolsCombination}
import de.evaluation.f1.GoldStandard
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.multiarmed.bandit.{MultiarmedBanditsExperimentBase, ToolExpectation}
import org.apache.spark.sql.{KeyValueGroupedDataset, Row}


/**
  * Created by visenger on 13/03/17.
  */


case class BanditAlgorithmResult(algName: String, param: Double, expectations: Seq[ToolExpectation])

case class BanditResults(dataSetName: String, banditsResults: Seq[BanditAlgorithmResult])

object ToolsAggregatorRunner
  extends MultiarmedBanditsExperimentBase
    with ExperimentsCommonConfig {

  def convert(expectations: String): Seq[ToolExpectation] = {
    //val Array(a, b, c, d, e) = expectations.split("\\|")
    //1:0.4143 -> toolId:expectation
    val resultOfBanditRun: Seq[ToolExpectation] =
    expectations.split("\\|").toList
      .map(e => new ToolExpectation().apply(e))
//    val resultOfBanditRun: Seq[ToolExpectation] =
//      expectations.split("\\|").toList
//        .map(e => new ToolExpectation().apply(e))
    resultOfBanditRun
  }

  def getParam(paramAsString: String) = {
    if (Strings.isNullOrEmpty(paramAsString))
      0.0
    else
      paramAsString.toDouble
  }

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("AGGREGATOR") {
      session => {
        import session.implicits._
        // val experimentsCSV = DataSetCreator.createFrame(session, multiArmedBandResults, schema: _*)
        val experimentsCSV = DataSetCreator.createFrame(session, multiArmedBandResults, schema: _*)

        val groupedDataset: KeyValueGroupedDataset[String, Row] = experimentsCSV.groupByKey(row => row.getAs[String]("dataset"))

        //dataset,banditalg,param,expectations
        val banditsForEachDataset: Array[BanditResults] = groupedDataset.mapGroups((dataset, bandits) => {

          val banditAlgorithmResults = bandits.toSeq.map(row => {

            val banditRow: Map[String, String] = row.getValuesMap[String](schema)

            val banditalg: String = banditRow.getOrElse("banditalg", "")
            val paramAsString = banditRow.getOrElse("param", "0.0")
            val param: Double = getParam(paramAsString)
            val expectationsAsString: String = banditRow.getOrElse("expectations", "")
            val toolExpectations: Seq[ToolExpectation] = convert(expectationsAsString)
            BanditAlgorithmResult(banditalg, param, toolExpectations)
          })

          BanditResults(dataset, banditAlgorithmResults)

        }).collect()

        // banditsForEachDataset.toSeq.foreach(println)

        //process collected results for each dataset:

        banditsForEachDataset.foreach(dataset => {
          println(s"processing: ${dataset.dataSetName}")
          val algorithmResults: Seq[BanditAlgorithmResult] = dataset.banditsResults

          val toolsCombinationsByAlgorithm = algorithmResults.flatMap(alg => {
            val algName: String = alg.algName
            val sortedExpectations: Seq[ToolExpectation] = alg.expectations
              .sortWith((e1, e2) => e1.expectation > e2.expectation)
            val toolsNumber = sortedExpectations.size

            val toolsCombinations = (3 until toolsNumber).map(howManyTools => {
              val combi: Seq[ToolExpectation] = sortedExpectations.take(howManyTools).toSeq
              val toolsCombi = combi
                .map(tool => s"${GoldStandard.exists}-${tool.id}")
                //.map(tool => getExtName(tool))
                .map(tool => Tool(tool))
              ToolsCombination(toolsCombi.toList)
            }).toSet.toSeq

            val combisByAlg = toolsCombinations.map(tool => tool -> algName)
            combisByAlg
          })
          val groupedByCombi: Map[ToolsCombination, Seq[(ToolsCombination, String)]] = toolsCombinationsByAlgorithm.groupBy(pair => pair._1)

          val toolsCombiAndAlgs: Map[ToolsCombination, Seq[String]] = groupedByCombi.map(pair => {
            val key = pair._1
            val algsWhichDetectedThisCombi = pair._2.map(_._2)
            key -> algsWhichDetectedThisCombi
          })

          toolsCombiAndAlgs.foreach(println)
        })


      }
    }
  }


}