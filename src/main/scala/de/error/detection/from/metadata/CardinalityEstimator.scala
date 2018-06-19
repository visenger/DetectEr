package de.error.detection.from.metadata

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.schema.Schema
import de.evaluation.f1.FullResult
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_set, count}

import scala.collection.convert.decorateAsScala._

class CardinalityEstimator {
  private var datasetName: String = ""
  private var fullMetadataDF: DataFrame = null
  private var dirtyDF: DataFrame = null
  private var datasetConfig: Config = null
  private var schema: Schema = null
  private var uniqueColumns: Seq[String] = Seq()

  def dataSetName(name: String): this.type = {
    datasetName = name
    datasetConfig = ConfigFactory.load(s"$name.conf")
    uniqueColumns = datasetConfig.getStringList("column.unique").asScala.toSeq
    this
  }

  def onSchema(datasetSchema: Schema): this.type = {
    schema = datasetSchema
    this
  }

  def forFullMetadataDF(dirtyDataDF: DataFrame): this.type = {
    fullMetadataDF = dirtyDataDF
    this
  }

  def forDirtyDataDF(dirtyDataDF: DataFrame): this.type = {
    dirtyDF = dirtyDataDF
    this
  }

  def getTuplesViolatedCardinality(): List[String] = {
    val listOfAttsViolatesCardinality: List[String] = fullMetadataDF
      .select("attrName")
      .where(fullMetadataDF("attrName").isin(uniqueColumns: _*) && fullMetadataDF("percentage of distinct vals") =!= 100)
      .collect().map(r => r.getString(0)).toList

    println(s" this attr violates cardinality: ${listOfAttsViolatesCardinality.mkString(",")}")

    //todo: we identify all tuples of unique attributes which violates cadinality
    val allTuplesViolatingCardinality: List[String] = listOfAttsViolatesCardinality.flatMap(attr => {

      dirtyDF.where(dirtyDF("attrName") === attr)
        .groupBy(dirtyDF(FullResult.value))
        .agg(collect_set(dirtyDF(schema.getRecID)).as("tid-set"), count(dirtyDF(schema.getRecID)).as("count"))
        .where(col("count") > 1)
        .select(col("tid-set"))
        .rdd
        .flatMap(row => row.getSeq[String](0))
        .collect()

    }).distinct

    allTuplesViolatingCardinality
  }


}

object CardinalityEstimator {
  def apply(): CardinalityEstimator = new CardinalityEstimator()
}