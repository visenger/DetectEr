package de.evaluation.data.util

import de.evaluation.f1.FullResult
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * General solulution for any number of tools.
  * Creates full result matrix of the form:
  * +-----+------+-----+--------+--------+--------+--------+--------+
  * |RecID|attrNr|label|exists-1|exists-2|exists-3|exists-4|exists-5|
  * +-----+------+-----+--------+--------+--------+--------+--------+
  * |10007|     3|    0|       0|       0|       0|       0|       0|
  * | 1001|     9|    0|       0|       0|       0|       0|       0|
  * |10034|     2|    0|       0|       0|       0|       0|       0|
  * |10034|     8|    0|       1|       0|       0|       0|       0|
  * |10042|    10|    0|       0|       0|       0|       0|       0|
  *
  */
class FullResultCreator(var session: SparkSession) {

  private var groundTruth: DataFrame = null
  private var toolsResults: Seq[DataFrame] = Nil
  private val schema = FullResult.schema

  def onGroundTruth(df: DataFrame): this.type = {
    groundTruth = df
    this
  }

  def onToolsResults(res: Seq[DataFrame]): this.type = {
    if (res.length != FullResult.toolsNumber)
      println("check the number of tools in the config file (e.g application.conf)")

    toolsResults = res
    this
  }

  def getFullResult: DataFrame = {
    import SchemaUtil._

    val extendAllWithColumn: Seq[DataFrame] = toolsResults
      .map(r => extendWithExistsColumn(session, groundTruth, r))

    val allJoins: DataFrame = extendAllWithColumn
      .foldLeft(groundTruth)((acc, tool) => acc.join(tool, joinCols))
    allJoins.toDF(schema: _*)
  }


}
