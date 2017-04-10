package de.wrangling

import com.typesafe.config.ConfigFactory
import de.evaluation.util.SparkLOAN
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by visenger on 06/04/17.
  */
object WrangleJSON {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("JSON") {
      session => {
        val config = ConfigFactory.load()
        val jsonPath = config.getString("metadata.blackoak.path")

        val top10Values = extractTop10Values(session, jsonPath)
        top10Values.show(false)


      }
    }
  }

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
      .select("id", "top10_flat")

    typeAndTop10DF
  }
}

object OneHotEncoderPlayground {
  /**
    * A one-hot encoder that maps a column of category indices to a column of binary vectors, with
    * at most a single one-value per row that indicates the input category index.
    * For example with 5 categories, an input value of 2.0 would map to an output vector of
    * `[0.0, 0.0, 1.0, 0.0]`.
    * The last category is not included by default (configurable via [[OneHotEncoder!.dropLast]]
    * because it makes the vector entries sum up to one, and hence linearly dependent.
    * So an input value of 4.0 maps to `[0.0, 0.0, 0.0, 0.0]`.
    * Note that this is different from scikit-learn's OneHotEncoder, which keeps all categories.
    * The output vectors are sparse.
    *
    * @see [[StringIndexer]] for converting categorical values into category indices
    */
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("ONEHOTENCODER") {
      session => {
        val df = session.createDataFrame(Seq(
          (0, "a"),
          (1, "b"),
          (2, "c"),
          (3, "a"),
          (4, "a"),
          (5, "c")
        )).toDF("id", "category")

        val indexer = new StringIndexer()
          .setInputCol("category")
          .setOutputCol("categoryIndex")
          .fit(df)
        val indexed = indexer.transform(df)

        val encoder = new OneHotEncoder()
          .setInputCol("categoryIndex")
          .setOutputCol("categoryVec")
          .setDropLast(false)

        val encoded = encoder.transform(indexed)
        encoded.show(false)
        encoded.printSchema()
      }
    }
  }
}
