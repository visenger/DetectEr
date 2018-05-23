package de.evaluation.data.metadata

import de.model.util.NumbersUtil
import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by visenger on 23/03/17.
  */
class MetadataCreator {

  def extractTop10Values(session: SparkSession, jsonPath: String): DataFrame = {

    val jsonDF = session.read.json(jsonPath)
    //jsonDF.show(false)
    // jsonDF.printSchema()

    //    val metadataDF = jsonDF
    //      .select(
    //        jsonDF("columnCombination.columnIdentifiers.columnIdentifier"),
    //        jsonDF("statisticMap.Data Type.value"),
    //        jsonDF("statisticMap.Nulls.value"),
    //        jsonDF("statisticMap.Top 10 frequent items.value"),
    //        jsonDF("statisticMap.Frequency Of Top 10 Frequent Items.value"))

    val top10DF = jsonDF
      .select(
        jsonDF("columnCombination.columnIdentifiers.columnIdentifier").as("id"),
        jsonDF("statisticMap.Top 10 frequent items.value").as("top10"))


    val flattenTop10DF = top10DF
      .select(top10DF("id"), explode(top10DF("top10")).as("top10_flat"))

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

    val typeAndTop10DF = top10TransformedDF
      .withColumn("id", getAttrName(top10TransformedDF("id_flat")))
      .withColumnRenamed("top10_flat", "top10")
      .withColumnRenamed("id", "attrNameMeta")
      .select("attrNameMeta", "top10")

    typeAndTop10DF
  }


  def getFullMetadata(session: SparkSession, jsonPath: String): DataFrame = {
    import org.apache.spark.sql.functions._

    val jsonDF = session.read.json(jsonPath)

    def create_histogram = udf {
      (vals: mutable.Seq[String], freqs: mutable.Seq[Long]) => {
        /* we sorted all frequent values in descending order -> the first is the most frequent item */
        val histValues: mutable.Seq[String] = vals.zip(freqs).sortBy(_._2).reverse.map(_._1)
        histValues
      }

    }

    def get_probabilities = udf {
      (freqs: mutable.Seq[Long], numTuples: Double) => {
        val probs: mutable.Seq[Double] = freqs.map(f => {
          NumbersUtil.round(f / numTuples, scale = 4)
        })
        probs
      }
    }

    def get_name = udf {
      colName: mutable.Seq[String] => colName.head
    }


    var metadataDF = jsonDF
      .select(
        jsonDF("columnCombination.columnIdentifiers.columnIdentifier").as("column name"),
        //jsonDF("statisticMap.Data Type.value"),
        jsonDF("statisticMap.Nulls.value").as("nulls count"),
        jsonDF("statisticMap.Percentage of Nulls.value").as("% of nulls"),
        jsonDF("statisticMap.Percentage of Distinct Values.value").as("% of distinct vals"),
        jsonDF("statisticMap.Top 10 frequent items.value").as("top10"),
        jsonDF("statisticMap.Frequency Of Top 10 Frequent Items.value").as("freqTop10"),
        jsonDF("statisticMap.Number of Tuples.value").as("number of tuples"),
        jsonDF("statisticMap.Standard Deviation.value").as("standard deviation")
      )

    metadataDF = metadataDF
      .withColumn("histogram", create_histogram(col("top10"), col("freqTop10")))
      .withColumn("attrName", get_name(col("column name")))
      .withColumn("probability of top 10 values", get_probabilities(col("freqTop10"), col("number of tuples")))
      .drop("column name")

    metadataDF


  }

}

object MetadataCreator {
  def apply(): MetadataCreator = new MetadataCreator()
}
