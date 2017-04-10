package de.evaluation.data.metadata

import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by visenger on 23/03/17.
  */
class MetadataCreator {

  def extractTop10Values(session: SparkSession, jsonPath: String): DataFrame = {

    val jsonDF = session.read.json(jsonPath)
    //jsonDF.show(false)
    // jsonDF.printSchema()

    val metadataDF = jsonDF
      .select(
        jsonDF("columnCombination.columnIdentifiers.columnIdentifier"),
        jsonDF("statisticMap.Data Type.value"),
        jsonDF("statisticMap.Nulls.value"),
        jsonDF("statisticMap.Top 10 frequent items.value"),
        jsonDF("statisticMap.Frequency Of Top 10 Frequent Items.value"))


    //  metadataDF.printSchema()


    val top10DF = jsonDF
      .select(
        jsonDF("columnCombination.columnIdentifiers.columnIdentifier").as("id"),
        jsonDF("statisticMap.Top 10 frequent items.value").as("top10")
        /*jsonDF("statisticMap.Frequency Of Top 10 Frequent Items.value").as("top10Frequency")*/)
    //    top10DF.show(false)
    //    top10DF.printSchema()

    val flattenTop10DF = top10DF
      .select(top10DF("id"), explode(top10DF("top10")).as("top10_flat"))
    //flattenTop10DF.show(false)

    val getAttrName = udf {
      attr: String => {
        val nameOnly: String = attr.trim.split("\\s+").head
        val additionalChangesNeeded = nameOnly.contains("(")
        additionalChangesNeeded match {
          case true => {
            nameOnly.substring(0, nameOnly.indexOf("("))
          }
          case false => nameOnly
        }
      }
    }

    val top10TransformedDF = flattenTop10DF
      .select(explode(flattenTop10DF("id")).as("id_flat"), flattenTop10DF("top10_flat"))

    val typeAndTop10DF = top10TransformedDF.withColumn("id", getAttrName(top10TransformedDF("id_flat")))
      .withColumnRenamed("top10_flat", "top10")
      .withColumnRenamed("id", "attrNameMeta")
      .select("attrNameMeta", "top10")

    typeAndTop10DF
  }

}
