package de.evaluation.data.salaries

import de.evaluation.data.util.FullResultCreator
import de.evaluation.tools.deduplication.nadeef.SalariesDuplicatesHandler
import de.evaluation.tools.outliers.dboost.{SalariesGaussDBoostResults, SalariesHistDBoostResults}
import de.evaluation.tools.pattern.violation.TrifactaSalariesResults
import de.evaluation.tools.ruleviolations.nadeef.SalariesRulesVioRunner
import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 19/02/17.
  */
class SalariesFullResult {

  def createFullResults(): Unit = {
    SparkLOAN.withSparkSession("SALARIES-FULL") {
      session => {
        val groundTruth = SalariesGoldStandardRunner.getGroundTruth(session)
        val patternViolationResult: DataFrame = TrifactaSalariesResults.getResult(session)

        val dedupResult: DataFrame = SalariesDuplicatesHandler.getResults(session)

        val histOutliers = SalariesHistDBoostResults.getResults(session)

        val gaussOutliers = SalariesGaussDBoostResults.getResults(session)

        val rulesVioResults = SalariesRulesVioRunner.getResult(session)

        val allResults: Seq[DataFrame] = Seq(
          patternViolationResult,
          dedupResult,
          histOutliers,
          gaussOutliers,
          rulesVioResults)

        val fullResultCreator = new FullResultCreator(session)
        fullResultCreator.onGroundTruth(groundTruth)
        fullResultCreator.onToolsResults(allResults)
        val fullResult: DataFrame = fullResultCreator.getFullResult
        fullResult.show()

        //        val conf = ConfigFactory.load()
        //        val outputFolder = "result.salaries.full.result.folder"
        //        fullResult
        //          .coalesce(1)
        //          .write
        //          .format("com.databricks.spark.csv")
        //          .option("header", true)
        //          .save(s"${conf.getString(outputFolder)}")


      }
    }
  }

}

object SalariesFullResultRunner {
  def main(args: Array[String]): Unit = {
    new SalariesFullResult().createFullResults()
  }
}
