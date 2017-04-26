package de.evaluation.data.flights

import com.typesafe.config.ConfigFactory
import de.evaluation.data.util.FullResultCreator
import de.evaluation.tools.deduplication.nadeef.FlightsDeduplicatesHandler
import de.evaluation.tools.outliers.dboost.{FlightsGaussDBoostResults, FlightsHistDBoostResults}
import de.evaluation.tools.pattern.violation.TrifactaFlightsResults
import de.evaluation.tools.ruleviolations.nadeef.FlightsRulesVioRunner
import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 25.04.17.
  */
class FlightsFullResults {

  def createFullResult(): Unit = {
    SparkLOAN.withSparkSession("FLIGHTS-FULL") {
      session => {
        val flightsGroundTruth = FlightsGoldStandard.getGroundTruth(session)

        val patternViolationResult = TrifactaFlightsResults.getResults(session)
        val dedupResult = FlightsDeduplicatesHandler.getResult(session)
        val histOutliers = FlightsHistDBoostResults.getResult(session)
        val gaussOutliers = FlightsGaussDBoostResults.getResult(session)
        val rulesVioResult = FlightsRulesVioRunner.getResult(session)

        val allResults: Seq[DataFrame] = Seq(patternViolationResult,
          dedupResult,
          histOutliers,
          gaussOutliers,
          rulesVioResult)

        val fullResult = new FullResultCreator(session)
          .onGroundTruth(flightsGroundTruth)
          .onToolsResults(allResults)
          .getFullResult
        fullResult.show()

        val config = ConfigFactory.load()
        val fullResultOutputFolder = "result.flights.full.result.folder"

        fullResult
          .coalesce(1)
          .write
          .option("header", true)
          .format("com.databricks.spark.csv")
          .save(config.getString(fullResultOutputFolder))


      }
    }
  }

}

object FlightsFullResultsRunner {
  def main(args: Array[String]): Unit = {
    new FlightsFullResults().createFullResult()
  }

}
