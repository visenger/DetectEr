package de.experiments.statistics

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig


/**
  * Created by visenger on 08/03/17.
  */
class ArbitraryStatistics {

}

case class ToolPerformance(dataset: String, toolName: String, tp: Long, fp: Long, fn: Long)

object PerformanceStatistics extends ExperimentsCommonConfig {


  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("STAT") {

      session => {
        import session.implicits._
        process_train_data {
          dataset => {
            val datasetName = dataset._1
            val fullDataFrame = DataSetCreator.createFrame(session, dataset._2, FullResult.schema: _*)

            val toolsPerformance: Seq[ToolPerformance] = FullResult.tools.map(tool => {
              val toolResult = fullDataFrame.select(tool, FullResult.label)
              val countResult =
                toolResult
                  .rdd
                  .map(row => (row.getString(0).toDouble, row.getString(1).toDouble))
                  .countByValue()
              //(detected as error : real error)
              val tp: Long = countResult.getOrElse((1.0, 1.0), 0L)
              val fp: Long = countResult.getOrElse((1.0, 0.0), 0L)
              val fn: Long = countResult.getOrElse((0.0, 1.0), 0L)

              ToolPerformance(datasetName, getName(tool), tp, fp, fn)

            })

            /**
              *
              * Map<String, Long> truePositives = new HashMap<String, Long>() {
              * {
              * put("trifacta", 5359L);
              * put("nadeef(dedup)", 4355L);
              * put("dboost(hist)", 326L);
              * put("dboost(gauss)", 6702L);
              * put("nadeef(fd)", 6891L);
              * }
              * };
              **/

            val groupedByName = toolsPerformance.groupBy(t => t.dataset)
            groupedByName.foreach(dataSetName => {

              val toolPerformances: Seq[ToolPerformance] = dataSetName._2
              val allTPs = toolsPerformance.map(t => s"""put("${t.toolName}", ${t.tp}L);""").mkString(" ")
              val allFPs = toolsPerformance.map(t => s"""put("${t.toolName}", ${t.fp}L);""").mkString(" ")
              val allFNs = toolsPerformance.map(t => s"""put("${t.toolName}", ${t.fn}L);""").mkString(" ")

              val mapTP =
                s"""
                   |Map<String, Long> ${datasetName}TruePositives = new HashMap<String, Long>() {
                   |            {
                   |              $allTPs
                   |            }
                   |        };
                   |
               """.stripMargin

              val mapFP =
                s"""
                   |Map<String, Long> ${datasetName}FalsePositives = new HashMap<String, Long>() {
                   |            {
                   |              $allFPs
                   |            }
                   |        };
                   |
               """.stripMargin

              val mapFN =
                s"""
                   |Map<String, Long> ${datasetName}FalseNegatives = new HashMap<String, Long>() {
                   |            {
                   |              $allFNs
                   |            }
                   |        };
                   |
               """.stripMargin


              println(mapTP)
              println(mapFP)
              println(mapFN)
              println("//******************************")

            })
          }
        }
      }
    }
  }
}
