package de.model.multiarmed.bandit

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{Eval, F1, FullResult, GoldStandard}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.immutable.Seq


/**
  * Multiarmed Bandit Experiments Evaluation.
  * the complete code for experiment:
  * https://github.com/visenger/banditsbook-scala
  */
trait MultiarmedBanditsExperimentBase {

  val config = ConfigFactory.load("experiments.conf")
  ///Users/visenger/research/datasets/multiarmed-bandit/experiments-10percent.csv

  val multiArmedBandResults = config.getString("multiarmed.bandit.result.csv")

  def schema = {
    val header = config.getString("multiarmed.bandit.header")
    val Array(dataset, banditalg, param, expectations) = header.split(",")
    Seq(dataset, banditalg, param, expectations)
  }

  def getTestDatasetPath(data: String): String = {

    val blackoakTestFile = config.getString("blackoak.experiments.test.file")
    val extBlackoakTestFile = config.getString("ext.blackoak.experiments.test.file")
    val hospTestFile = config.getString("hosp.experiments.test.file")
    val salariesTestFile = config.getString("salaries.experiments.test.file")

    val path = data.toLowerCase() match {
      case "blackoak" => blackoakTestFile
      case "ext.blackoak" => extBlackoakTestFile
      case "hosp" => hospTestFile
      case "salaries" => salariesTestFile
      case _ => ""
    }
    path
  }


}

class MultiarmedBandit {

}

case class ToolExpectation(id: Int = 0, expectation: Double = 0.0) {
  def apply(id: Int, expectation: Double): ToolExpectation = new ToolExpectation(id, expectation)

  def apply(tool: String): ToolExpectation = {
    val Array(idStr, expectStr) = tool.split(":")

    new ToolExpectation(id = Integer.valueOf(idStr), expectation = java.lang.Double.valueOf(expectStr))
  }
}

@Deprecated
object MultiarmedBanditRunner extends MultiarmedBanditsExperimentBase {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("MULTIARMEDBANDIT") {
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
              (1 to sortedResults.size)
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

        val allDatasets: Array[String] = experimentsCSV
          .select(experimentsCSV.col("dataset"))
          .distinct()
          .map(row => row.getString(0))
          .collect()

        allDatasets.foreach(data => {
          val path = getTestDatasetPath(data)
          val fullResult: DataFrame = DataSetCreator.createFrame(session, path, FullResult.schema: _*)

          val experimentsByDataset: Dataset[Row] =
            experimentSettings
              .where(experimentSettings.col("dataset") === data)

          val allCombis: Array[String] = experimentsByDataset
            .select(experimentSettings.col("toolscombi"))
            .distinct()
            .map(row => row.getString(0))
            .collect()

          val toolsCombinations: List[List[String]] = allCombis.map(l => {
            val toolsAsList: List[String] = l.split(",").toList
            toolsAsList
          }).toSeq.toList

          println(s"EVALUATING: $data")
          toolsCombinations.foreach(topTools => {
            //println(s"""TOOLS COMBI: ${topTools.mkString("+")}""")
            val label = FullResult.label
            val labelAndTopTools = fullResult.select(label, topTools: _*)
            val eval: Eval = F1.evaluate(labelAndTopTools)
            //eval.printResult("union all")


            val k = topTools.length
            val minK: Eval = F1.evaluate(labelAndTopTools, k)
            //minK.printResult(s"min-$k")
            val latexTableRow =
              s"""
                  \\multirow{2}{*}{Top-1} & \\multirow{2}{*}{${topTools.mkString("+")}} & union all  & ${eval.precision}        & ${eval.recall}     & ${eval.f1}  \\\\
                             &                              & min-$k      & ${minK.precision}        & ${minK.recall}     & ${minK.f1}   \\\\
          """.stripMargin
            println(latexTableRow)

          })


        })


      }
    }
  }
}


/**
  *
  *
  * +--------+--------------+-----+--------------------+
  * | dataset|     banditalg|param|        expectations|
  * +--------+--------------+-----+--------------------+
  * |blackoak|epsilon-greedy|  0.8|1:0.4143|2:0.3336...|
  * |    hosp|epsilon-greedy| 0.95|1:0.8415|2:0.1273...|
  * |salaries|epsilon-greedy| 0.95|1:0.0|2:0.0764|3:...|
  * |blackoak|       softmax|  0.1|1:0.4265|2:0.3021...|
  * |    hosp|       softmax|  0.1|1:0.8386|2:0.0|3:...|
  * |salaries|       softmax|  0.1|1:0.0106|2:0.0901...|
  * |blackoak|          exp3|  0.3|1:4.4380869264469...|
  * |    hosp|          exp3|  0.2|1:9.3943412930196...|
  * |salaries|          exp3|  0.3|1:2.7182818284590...|
  * |blackoak|           ucb| null|1:0.4383|2:0.2862...|
  * |    hosp|           ucb| null|1:0.8376|2:0.0969...|
  * |salaries|           ucb| null|1:0.0052|2:0.1067...|
  * +--------+--------------+-----+--------------------+
  *
  *
  *
  *
  *
  **/