package de.evaluation.data.metadata

import de.evaluation.f1.FullResult
import de.model.util.NumbersUtil
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions.{collect_list, explode, udf}
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
        jsonDF("statisticMap.Number of Tuples.value").as("number of tuples")
        //jsonDF("statisticMap.Standard Deviation.value").as("standard deviation")
      )

    metadataDF = metadataDF
      .withColumn("histogram", create_histogram(col("top10"), col("freqTop10")))
      .withColumn("attrName", get_name(col("column name")))
      .withColumn("probability of top 10 values", get_probabilities(col("freqTop10"), col("number of tuples")))
      .drop("column name")

    metadataDF


  }

  def getMetadataWithCounts(session: SparkSession, jsonPath: String, dirtyDF: DataFrame): DataFrame = {
    val metadata: DataFrame = getFullMetadata(session, jsonPath)
    val valuesWithCounts: DataFrame = collectValuesWithCounts(dirtyDF)

    val metadataWithCounts: DataFrame = metadata.join(valuesWithCounts, Seq("attrName"))
    metadataWithCounts
  }

  private def collectValuesWithCounts(dirtyDF: DataFrame): DataFrame = {
    val count: Long = dirtyDF.count()

    val aggrDirtyDF: DataFrame = dirtyDF.groupBy("attrName")
      .agg(collect_list(FullResult.value).as("column-values"))

    def compute_pattern_length = udf {
      columnValues: mutable.Seq[String] => {
        columnValues.map(value => value.length)
      }
    }

    def compute_pattern_min = udf {
      columnValues: mutable.Seq[String] => {

        val set: Set[String] = columnValues.toSet
        set.map(value => value.length).min
      }
    }

    def compute_pattern_max = udf {
      columnValues: mutable.Seq[String] => {
        val set: Set[String] = columnValues.toSet
        set.map(value => value.length).max
      }
    }

    val patternStatisticsDF: DataFrame = aggrDirtyDF
      .withColumn("pattern-length", compute_pattern_length(aggrDirtyDF("column-values")))
      .withColumn("pattern-length-min", compute_pattern_min(aggrDirtyDF("column-values")))
      .withColumn("pattern-length-max", compute_pattern_max(aggrDirtyDF("column-values")))
      .drop("column-values")


    val countVectorizerModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("column-values")
      .setOutputCol("features")
      .setVocabSize(count.toInt)
      .fit(aggrDirtyDF)

    val frequentValsDF: DataFrame = countVectorizerModel.transform(aggrDirtyDF)

    val vocabulary: Array[String] = countVectorizerModel.vocabulary
    val indexToValsDictionary: Map[Int, String] = vocabulary.zipWithIndex.map(_.swap).toMap

    def extract_counts = udf {
      features: org.apache.spark.ml.linalg.Vector => {

        val sparse: SparseVector = features.toSparse
        //val totalVals: Int = sparse.size
        val indices: Array[Int] = sparse.indices
        val values: Array[Double] = sparse.values
        val tuples: Seq[(String, Int)] = indices.zip(values)
          // .filter(_._2 > 1.0)
          .sortWith(_._2 > _._2)
          // .map(v => (indexToValsDictionary(v._1), NumbersUtil.round(v._2 / totalVals, 4)))
          .map(v => (indexToValsDictionary(v._1), v._2.toInt))
          .toSeq

        tuples.toMap
      }
    }

    def extract_total_number_of_vals = udf {
      features: org.apache.spark.ml.linalg.Vector => {
        val sparse: SparseVector = features.toSparse
        val countOfValues: Int = sparse.indices.length
        countOfValues
      }
    }

    val valuesWithCounts = "values-with-counts"
    val distinctValsCount = "distinct-vals-count"
    val withNewFeatures: DataFrame = frequentValsDF
      .withColumn(valuesWithCounts, extract_counts(frequentValsDF("features")))
      .withColumn(distinctValsCount, extract_total_number_of_vals(frequentValsDF("features")))
      .select("attrName", distinctValsCount, valuesWithCounts)

    val newFeaturesWithPatternStatsDF: DataFrame = withNewFeatures
      .join(patternStatisticsDF, Seq("attrName"))

    newFeaturesWithPatternStatsDF
  }

}

object MetadataCreator {
  def apply(): MetadataCreator = new MetadataCreator()
}
