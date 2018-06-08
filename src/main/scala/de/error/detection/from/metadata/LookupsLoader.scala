package de.error.detection.from.metadata

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.schema.Schema
import de.evaluation.data.util.LookupColumns
import de.evaluation.f1.FullResult
import de.evaluation.util.DataSetCreator
import org.apache.spark.sql.functions.{col, isnull, trim, upper}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.JavaConversions._

import scala.collection.immutable

class LookupsLoader {
  private var datasetName: String = ""
  private var dirtyDF: DataFrame = null
  private var datasetConfig: Config = null
  private var schema: Schema = null

  def dataSetName(name: String): this.type = {
    datasetName = name
    datasetConfig = ConfigFactory.load(s"$name.conf")
    this
  }

  def onSchema(datasetSchema: Schema): this.type = {
    schema = datasetSchema
    this
  }

  def forDirtyDataDF(dirtyDataDF: DataFrame): this.type = {
    dirtyDF = dirtyDataDF
    this
  }

  def getLookupColumns(): Seq[String] = {
    val lookupCols: List[Config] = datasetConfig.getConfigList("lookup.columns").toList
    val lookupColumnNames: immutable.Seq[String] = lookupCols.map(_.getString("name")).toSeq
    lookupColumnNames
  }

  def load(session: SparkSession): Map[String, Seq[String]] = {
    val lookupCols: List[Config] = datasetConfig.getConfigList("lookup.columns").toList
    val lookups: List[LookupColumns] = lookupCols
      .map(c => LookupColumns(c.getString("name"), c.getString("source")))

    val lookupByAttr: Map[String, Seq[String]] = lookups.map(l => {
      val name: String = l.colName
      val source: String = l.pathToLookupSource

      val lookupDF: DataFrame = DataSetCreator.createFrame(session, source, Seq(s"lookup-$name"): _*)

      val lookupWithDirtyDF: DataFrame = dirtyDF.where(dirtyDF("attrName") === name)
        .join(lookupDF,
          upper(trim(dirtyDF(FullResult.value))) === upper(trim(lookupDF(s"lookup-$name"))),
          "left_outer")

      val allIDsNotInLookup: Seq[String] = lookupWithDirtyDF
        .select(schema.getRecID)
        .where(isnull(col(s"lookup-$name")))
        .collect()
        .map(row => row.getString(0))
        .toSeq
      (name, allIDsNotInLookup)
    }).toMap

    lookupByAttr
  }


}

object LookupsLoader {
  def apply(): LookupsLoader = new LookupsLoader()
}