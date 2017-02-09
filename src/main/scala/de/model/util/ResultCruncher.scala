package de.model.util

import com.typesafe.config.ConfigFactory
import de.evaluation.data.blackoak.{BlackOakGoldStandard, BlackOakGoldStandardRunner}
import de.evaluation.f1.{DataF1, Table}
import de.evaluation.tools.deduplication.nadeef.NadeefDeduplicationResults
import de.evaluation.tools.outliers.dboost.DBoostResults
import de.evaluation.tools.pattern.violation.TrifactaResults
import de.evaluation.tools.ruleviolations.nadeef.NadeefRulesVioResults
import de.evaluation.util.{DataSetCreator, DatabaseProps, SparkLOAN, SparkSessionCreator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.collection.immutable.IndexedSeq


/**
  * Created by visenger on 22/12/16.
  */
class ResultCruncher {


  def createMatrixForModel() = {
    import SparkLOAN._
    // disadvanatage: with loan pattern we are not able to use the data frame objects outside the scope
    // of the method
    // loan pattern ist suitable for performing something without returning result. -> void methods.
    // e.g side-effect methods, which perform I/O;
    withSparkSession("cruncher") {
      sparkSession => {
        val goldStandard: DataFrame = BlackOakGoldStandardRunner
          .getGoldStandard(sparkSession)

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

        val patternVioExists: DataFrame = extendWithExistsColumn(sparkSession, goldStandard, patternViolationResult)
        val dedupExists: DataFrame = extendWithExistsColumn(sparkSession, goldStandard, dedupResult)
        val histExists: DataFrame = extendWithExistsColumn(sparkSession, goldStandard, histOutliers)
        val gaussExists: DataFrame = extendWithExistsColumn(sparkSession, goldStandard, gaussOutliers)
        val rulesVioExists: DataFrame = extendWithExistsColumn(sparkSession, goldStandard, rulesVioResults)

        val columnsOnJoin = Seq(Table.recid, Table.attrnr)
        val tools: Seq[String] = (1 to 5).map(i => s"${Table.exists}-$i").toSeq

        val join = patternVioExists.join(dedupExists, columnsOnJoin)
          .join(histExists, columnsOnJoin)
          .join(gaussExists, columnsOnJoin)
          .join(rulesVioExists, columnsOnJoin)
          .toDF(columnsOnJoin ++ tools: _*)

        join.show(5)

        /* val conf = ConfigFactory.load()
         join
           .coalesce(1)
           .write
           .format("com.databricks.spark.csv")
           .option("header", true)
           .save(s"${conf.getString("model.matrix.folder")}")*/
      }

    }


  }


  private def extendWithExistsColumn(sparkSession: SparkSession, goldStandard: DataFrame, errorDetectResult: DataFrame) = {
    import sparkSession.implicits._

    val ones = goldStandard.intersect(errorDetectResult)
    val existsDF: DataFrame = ones
      .map(row => (row.getString(0), row.getString(1), "1")).toDF(Table.schema: _*)

    val zeros: Dataset[Row] = goldStandard.except(errorDetectResult)
    val notExistsDF: DataFrame = zeros
      .map(row => (row.getString(0), row.getString(1), "0")).toDF(Table.schema: _*)

    val union: DataFrame = existsDF.union(notExistsDF).toDF(Table.schema: _*)
    union
  }


  private def inLibsvmFormat(sparkSession: SparkSession,
                             goldStandard: DataFrame,
                             errorDetectResult: DataFrame,
                             label: String) = {
    import sparkSession.implicits._

    val ones = goldStandard.intersect(errorDetectResult)


    /**/
    val existsDF: DataFrame = ones
      .map(row => (row.getString(0), row.getString(1), "1")).toDF(Table.schema: _*)

    existsDF
  }
}

object ResultCruncher1 {
  def main(args: Array[String]): Unit = {

    new ResultCruncher().createMatrixForModel()
  }

}
