package de.evaluation.data.hosp

import com.typesafe.config.ConfigFactory
import de.evaluation.data.blackoak.BlackOakGoldStandardRunner
import de.evaluation.data.util.SchemaUtil
import de.evaluation.f1.{FullResult, GoldStandard}
import de.evaluation.tools.deduplication.nadeef.{HospDuplicatesHandler, NadeefDeduplicationResults}
import de.evaluation.tools.outliers.dboost.{DBoostResults, HospGaussDBoostResults, HospHistDBoostResults}
import de.evaluation.tools.pattern.violation.{TrifactaHospResults, TrifactaResults}
import de.evaluation.tools.ruleviolations.nadeef.{HospRulesVioRunner, NadeefRulesVioResults}
import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 16/02/17.
  */
class HospFullResult {

  def createFullResults(): Unit = {
    SparkLOAN.withSparkSession("HOSPFULL") {
      session => {
        val groundTruth: DataFrame = HospGoldStandardRunner.getGroundTruth(session)

        val patternViolationResult: DataFrame = TrifactaHospResults.getResult(session)

        val dedupResult: DataFrame = HospDuplicatesHandler.getResults(session)

        val histOutliers = HospHistDBoostResults.getResults(session)

        val gaussOutliers = HospGaussDBoostResults.getResults(session)

        val rulesVioResults = HospRulesVioRunner.getResult(session)


        val pattern: DataFrame = SchemaUtil.extendWithExistsColumn(session, groundTruth, patternViolationResult)
        val dedup: DataFrame = SchemaUtil.extendWithExistsColumn(session, groundTruth, dedupResult)
        val hist: DataFrame = SchemaUtil.extendWithExistsColumn(session, groundTruth, histOutliers)
        val gauss: DataFrame = SchemaUtil.extendWithExistsColumn(session, groundTruth, gaussOutliers)
        val rules: DataFrame = SchemaUtil.extendWithExistsColumn(session, groundTruth, rulesVioResults)

        //val tools: Seq[String] = (1 to 5).map(i => s"${GoldStandard.exists}-$i")
        val schema = FullResult.schema //SchemaUtil.joinCols ++ Seq(FullResult.label) ++ tools

        val fullResult: DataFrame = groundTruth
          .join(pattern, SchemaUtil.joinCols)
          .join(dedup, SchemaUtil.joinCols)
          .join(hist, SchemaUtil.joinCols)
          .join(gauss, SchemaUtil.joinCols)
          .join(rules, SchemaUtil.joinCols)
          .toDF(schema: _*)

        val outputFolder="result.hosp.10k.full.result.folder"
        val conf= ConfigFactory.load()
        fullResult
          .coalesce(1)
          .write
          .format("com.databricks.spark.csv")
          .option("header", true)
          .save(s"${conf.getString(outputFolder)}")


      }
    }
  }


}

object HospFullResultRunner {
  def main(args: Array[String]): Unit = {
    new HospFullResult().createFullResults()
  }
}
