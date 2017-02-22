package de.model.multiarmed.bandit

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by visenger on 21/02/17.
  */
trait CommonBase {

  val blackOakFullResultFile = "output.full.result.file"
  val hospFullResultFile = "result.hosp.10k.full.result.file"
  val salariesFullResultFile = "result.salaries.full.result.file"

  val allResults = Map[String, String](
    ("BLACKOAK" -> blackOakFullResultFile),
    ("HOSP" -> hospFullResultFile),
    ("SALARIES" -> salariesFullResultFile))

  val fullResultSchema = FullResult

  def process_data(f: Tuple2[String, String] => DataFrame): Seq[DataFrame] = {
    allResults.map(t => f(t)).toSeq
  }

  case class Result(dataSetName: String, tool1: String, tool2: String, tool3: String, tool4: String, tool5: String)

}

object RewardSchema extends CommonBase {
  val datasetName = "dataset"
  val tools = fullResultSchema.tools

  val schema = Seq("dataset", "toolid", "probability")


}

object ToolsRewards extends CommonBase {

  import de.model.util.NumbersUtil._


  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("REWARDCREATOR") {
      session => {
        import session.implicits._
        val allDataSetsAndProbs: Seq[DataFrame] = process_data {
          data => {

            val datasetName = data._1

            val configPath = data._2
            val toolsResultDF = DataSetCreator.createDataSetFromCSV(session, configPath, fullResultSchema.schema: _*)
            //toolsResultDF.show()

            val allTools: Seq[String] = fullResultSchema.tools

            val toolsToProbabilities: Seq[(String, String, String)] = allTools.map(tool => {
              val labelsAndTool = toolsResultDF.select(fullResultSchema.label, tool)

              val correctIdentifiedErrors: Long = labelsAndTool
                .where(toolsResultDF.col(fullResultSchema.label) === "1" && toolsResultDF.col(tool) === "1")
                .count()
              val totalIdentifiedErrors = labelsAndTool.where(toolsResultDF.col(tool) === "1").count()
              val probabilityOfError: Double = correctIdentifiedErrors.toDouble / totalIdentifiedErrors.toDouble

              (datasetName, tool, String.valueOf(round(probabilityOfError, 3)))
            })

            //implicit val rowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
            toolsToProbabilities.toDF(RewardSchema.schema: _*)
          }
        }


        val allProbsDF: DataFrame = allDataSetsAndProbs
          .reduce((df1, df2) => df1.union(df2))
          .toDF()
        //todo: persist

        allProbsDF.show()
      }
    }
  }

}
