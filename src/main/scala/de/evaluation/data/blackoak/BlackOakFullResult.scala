package de.evaluation.data.blackoak

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{DataF1, FullResult, Table}
import de.evaluation.tools.deduplication.nadeef.NadeefDeduplicationResults
import de.evaluation.tools.outliers.dboost.DBoostResults
import de.evaluation.tools.pattern.violation.TrifactaResults
import de.evaluation.tools.ruleviolations.nadeef.NadeefRulesVioResults
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.catalyst.plans.{Inner, RightOuter}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Aggregates all results from the data cleaning tools;
  */
class BlackOakFullResult {

  private val joinCols = Seq(Table.recid, Table.attrnr)

  def createFullResults(): Unit = {
    SparkLOAN.withSparkSession("FULL") {
      sparkSession => {

        val groundTruth: DataFrame = BlackOakGoldStandardRunner
          .getGroundTruth(sparkSession)

        val patternViolationResult: DataFrame = TrifactaResults
          .getPatternViolationResult(sparkSession)

        val dedupResult: DataFrame = NadeefDeduplicationResults
          .getDedupResults(sparkSession)

        val histOutliers = DBoostResults
          .getHistogramResultForOutlierDetection(sparkSession)

        val gaussOutliers = DBoostResults
          .getGaussResultForOutlierDetection(sparkSession)

        val rulesVioResults = NadeefRulesVioResults
          .getRulesVioResults(sparkSession)


        val pattern: DataFrame = extendWithExistsColumn(sparkSession, groundTruth, patternViolationResult)
        val dedup: DataFrame = extendWithExistsColumn(sparkSession, groundTruth, dedupResult)
        val hist: DataFrame = extendWithExistsColumn(sparkSession, groundTruth, histOutliers)
        val gauss: DataFrame = extendWithExistsColumn(sparkSession, groundTruth, gaussOutliers)
        val rules: DataFrame = extendWithExistsColumn(sparkSession, groundTruth, rulesVioResults)


        val tools: Seq[String] = (1 to 5).map(i => s"${Table.exists}-$i")
        val schema = joinCols ++ Seq(FullResult.label) ++ tools

        val fullResult: DataFrame = groundTruth
          .join(pattern, joinCols)
          .join(dedup, joinCols)
          .join(hist, joinCols)
          .join(gauss, joinCols)
          .join(rules, joinCols)
          .toDF(schema: _*)

        //header: RecID,attrNr,label,exists-1,exists-2,exists-3,exists-4,exists-5

        val conf = ConfigFactory.load()
        fullResult
          .coalesce(1)
          .write
          .format("com.databricks.spark.csv")
          .option("header", true)
          .save(s"${conf.getString("output.full.result.folder")}")


      }
    }
  }


  private def extendWithExistsColumn(sparkSession: SparkSession, goldStd: DataFrame, errorDetectResult: DataFrame) = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    /* goldStd is already in Table format with exists column */

    val selection = goldStd.select(Table.recid, Table.attrnr)
    val ones = selection.intersect(errorDetectResult)

    println(s" ones count ${ones.count()}")

    val existsDF: DataFrame = ones
      .map(row => (row.getString(0), row.getString(1), "1")).toDF(Table.schema: _*)

    val zeros: Dataset[Row] = selection.except(errorDetectResult)
    val notExistsDF: DataFrame = zeros
      .map(row => (row.getString(0), row.getString(1), "0")).toDF(Table.schema: _*)

    println(s" zeros count ${zeros.count()}")

    val union: DataFrame = existsDF.union(notExistsDF).toDF(Table.schema: _*)
    union
  }
}

object BlackOakFullResultRunner {

  def main(args: Array[String]): Unit = {

    new BlackOakFullResult().createFullResults()

  }


}


