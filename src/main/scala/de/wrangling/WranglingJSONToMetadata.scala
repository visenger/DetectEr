package de.wrangling

import com.typesafe.config.ConfigFactory
import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object WranglingJSONToMetadata {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("JSON") {
      session => {
        val config = ConfigFactory.load()
        val jsonPath = config.getString("metadata.flights.path")

        val top10Values = getFullMetadata(session, jsonPath)
        top10Values.show(false)
        top10Values.printSchema()


      }
    }
  }

  def getFullMetadata(session: SparkSession, jsonPath: String): DataFrame = {
    import org.apache.spark.sql.functions._

    val jsonDF = session.read.json(jsonPath)

    def create_histogram = udf {
      (vals: mutable.Seq[String], freqs: mutable.Seq[Long]) => {
        /* we sorted all frequent values in descending order -> the first is the most frequent item */
        vals.zip(freqs).sortBy(_._2).reverse.map(_._1)
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
        jsonDF("statisticMap.Frequency Of Top 10 Frequent Items.value").as("freqTop10")
      )

    metadataDF = metadataDF
      .withColumn("histogram", create_histogram(col("top10"), col("freqTop10")))
      .withColumn("attrName", get_name(col("column name")))

    metadataDF
  }
}
