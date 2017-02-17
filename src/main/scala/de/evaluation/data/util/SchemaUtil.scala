package de.evaluation.data.util

import de.evaluation.f1.GoldStandard
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Util class for full result creation.
  */
object SchemaUtil {

  val joinCols = Seq(GoldStandard.recid, GoldStandard.attrnr)

  def extendWithExistsColumn(sparkSession: SparkSession, goldStd: DataFrame, errorDetectResult: DataFrame): DataFrame = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    /* goldStd is already in Table format with exists column */

    val selection = goldStd.select(GoldStandard.recid, GoldStandard.attrnr)
    val ones = selection.intersect(errorDetectResult)

    println(s" ones count ${ones.count()}")

    val existsDF: DataFrame = ones
      .map(row => (row.getString(0), row.getString(1), "1")).toDF(GoldStandard.schema: _*)

    val zeros: Dataset[Row] = selection.except(errorDetectResult)
    val notExistsDF: DataFrame = zeros
      .map(row => (row.getString(0), row.getString(1), "0")).toDF(GoldStandard.schema: _*)

    println(s" zeros count ${zeros.count()}")

    val union: DataFrame = existsDF.union(notExistsDF).toDF(GoldStandard.schema: _*)
    union
  }

}
