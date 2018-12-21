package de.evaluation.data.metadata

import breeze.linalg.DenseVector
import breeze.stats._
import de.evaluation.f1.FullResult
import de.model.util.NumbersUtil
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by visenger on 23/03/17.
  *
  * Derives metadata features of the input columns to use as input to error detection routines.
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
        jsonDF("statisticMap.Percentage of Nulls.value").as("percentage of nulls"),
        jsonDF("statisticMap.Percentage of Distinct Values.value").as("percentage of distinct vals"),
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

    def compute_length_distribution = udf {
      valuesLength: mutable.Seq[Int] => {
        val lengthToCount: Map[Int, Int] = valuesLength
          .groupBy(length => length)
          .map(pair => (pair._1, pair._2.size))
          .toMap
        lengthToCount
      }
    }

    def compute_length_distribution_threshold = udf {
      lengthToCount: Map[Int, Int] => {
        val totalCount: Int = lengthToCount.size
        var result: Seq[Int] = lengthToCount.map(_._1).toSeq

        val threshold = 10
        if (totalCount >= threshold) {
          val howManyIs10Percent: Double = totalCount / threshold.toDouble
          val thresholdElements: Int = scala.math.ceil(howManyIs10Percent).toInt
          val remainingElements: Int = totalCount - thresholdElements
          result = lengthToCount
            .toSeq
            .sortWith(_._2 > _._2)
            .take(remainingElements)
            .map(_._1).toSeq
        }

        result
      }
    }

    def compute_pattern_length = udf {
      columnValues: mutable.Seq[String] => {
        val lengths: mutable.Seq[Int] = columnValues.map(value => value.length).sorted
        lengths
      }
    }

    def compute_pattern_min = udf {
      columnValues: mutable.Seq[String] => {
        var result = 0
        if (!columnValues.isEmpty) {
          val set: Set[String] = columnValues.toSet
          result = set.map(value => value.length).min
        }
        result
      }
    }

    def compute_pattern_max = udf {
      columnValues: mutable.Seq[String] => {
        var result = 0
        if (!columnValues.isEmpty) {
          val set: Set[String] = columnValues.toSet
          result = set.map(value => value.length).max
        }
        result

      }
    }

    def compute_length_trimmed_distr = udf {
      (valuesLength: mutable.Seq[Int], fraction: Double) => {

        var result = valuesLength

        if (valuesLength.nonEmpty) {
          val distrSize: Int = valuesLength.size
          val toRemovePercentage: Double = fraction * 100

          val elementsToRemove: Double = distrSize * toRemovePercentage / 100
          val roundedHalfUpElementsToRemove: Double = NumbersUtil.round(elementsToRemove, scale = 0)
          if (roundedHalfUpElementsToRemove < distrSize) {
            val toRemoveFromEachSide: Double = roundedHalfUpElementsToRemove / 2
            val roundedHalfUpRemoveFromEachSide: Int = NumbersUtil.round(toRemoveFromEachSide, scale = 0).toInt
            val sortedLenghts: mutable.Seq[Int] = valuesLength.sorted
            val trimmedLengthDistr: mutable.Seq[Int] = sortedLenghts.drop(roundedHalfUpRemoveFromEachSide).dropRight(roundedHalfUpRemoveFromEachSide)
            result = trimmedLengthDistr
          }
        }

        result

      }
    }

    def compute_length_winsorized = udf {
      (valuesLength: mutable.Seq[Int], fraction: Double) => {

        var result = valuesLength

        if (valuesLength.nonEmpty) {
          val distrSize: Int = valuesLength.size
          val toRemovePercentage: Double = fraction * 100

          val elementsToRemove: Double = distrSize * toRemovePercentage / 100
          val roundedHalfUpElementsToRemove: Double = NumbersUtil.round(elementsToRemove, scale = 0)
          if (roundedHalfUpElementsToRemove < distrSize) {
            val toRemoveFromEachSide: Double = roundedHalfUpElementsToRemove / 2
            val roundedHalfUpRemoveFromEachSide: Int = NumbersUtil.round(toRemoveFromEachSide, scale = 0).toInt
            val sortedLenghts: mutable.Seq[Int] = valuesLength.sorted
            val trimmedLengthDistr: mutable.Seq[Int] = sortedLenghts
              .drop(roundedHalfUpRemoveFromEachSide)
              .dropRight(roundedHalfUpRemoveFromEachSide)
            val maxRightElement: Int = trimmedLengthDistr.max
            val minLeftElement: Int = trimmedLengthDistr.min
            val rightWinsorized: mutable.Seq[Int] = mutable.Seq.fill(roundedHalfUpRemoveFromEachSide)(maxRightElement)
            val leftWinsorized: mutable.Seq[Int] = mutable.Seq.fill(roundedHalfUpRemoveFromEachSide)(minLeftElement)

            val winsorizedLenghts: mutable.Seq[Int] = leftWinsorized ++ trimmedLengthDistr ++ rightWinsorized
            result = winsorizedLenghts.sorted

          }
        }

        result

      }
    }


    val patternStatisticsDF: DataFrame = aggrDirtyDF
      .withColumn("pattern-length", compute_pattern_length(aggrDirtyDF("column-values")))
      .withColumn("pattern-length-dist-full", compute_length_distribution(col("pattern-length")))
      .withColumn("pattern-length-dist-10", compute_length_distribution_threshold(col("pattern-length-dist-full")))
      .withColumn("pattern-length-trimmed", compute_length_trimmed_distr(col("pattern-length"), lit(0.2)))
      .withColumn("pattern-length-winsorized", compute_length_winsorized(col("pattern-length"), lit(0.2)))

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


    def descriptive_statistics_pattern_length = udf {
      valuesLength: mutable.Seq[Int] => {
        val valsAsDouble: mutable.Seq[Double] = valuesLength.map(_.toDouble)
        val values: DenseVector[Double] = DenseVector[Double](valsAsDouble: _*)

        val minPatternLenght: Int = if (valuesLength.nonEmpty) valuesLength.min else 0
        val maxPatternLength: Int = if (valuesLength.nonEmpty) valuesLength.max else 0

        def computeMode(vals: DenseVector[Double]): Double = {
          val valsMode: Double = mode(values).mode
          val result: Double = if (valsMode.isNaN) 0.0 else valsMode
          result
        }

        val mostFrequentPatternLength: Double = computeMode(values)

        val mv = meanAndVariance(values)

        def computeMedian(valsAsInts: mutable.Seq[Int]): Int = {
          var result: Int = 0
          if (valsAsInts.nonEmpty) {
            result = median(DenseVector[Int](valsAsInts.toArray))
          }
          result
        }

        def computeQuartile(valsAsDouble: mutable.Seq[Double], p: Double): Double = {
          var result: Double = 0.0
          if (valsAsDouble.nonEmpty) {
            result = DescriptiveStats.percentile(valsAsDouble.toArray, p)
          }
          result
        }

        val medianPatternLengths: Int = computeMedian(valuesLength)
        val lowerQuartile: Double = computeQuartile(valsAsDouble, 0.25)
        val upperQuartile: Double = computeQuartile(valsAsDouble, 0.75)

        val sequenceMean: Double = mv.mean

        //compute MAD median absolute deviation for robust dispersion -> for Hampel x84 outliers detection
        val valsDiffMean: mutable.Seq[Int] = valuesLength.map(v => math.abs(v - medianPatternLengths))
        val mad: Double = computeMedian(valsDiffMean)

        Map(
          "median" -> medianPatternLengths.toDouble,
          "min-length" -> minPatternLenght.toDouble,
          "max-length" -> maxPatternLength.toDouble,
          "mean" -> sequenceMean,
          "var" -> mv.variance,
          "stddev" -> mv.stdDev,
          "most-freq-pattern-length" -> mostFrequentPatternLength,
          "lower-quartile" -> lowerQuartile,
          "upper-quartile" -> upperQuartile,
          "mad" -> mad)
      }
    }

    def descriptive_statistics_trimmed_pattern_length = udf {
      trimmedValuesLength: mutable.Seq[Int] => {

        val valsAsDouble: mutable.Seq[Double] = trimmedValuesLength.map(_.toDouble)
        val values: DenseVector[Double] = DenseVector[Double](valsAsDouble: _*)
        val mv = meanAndVariance(values)
        Map("trimmed-mean" -> mv.mean,
          "trimmed-std-dev" -> mv.stdDev)
      }
    }

    def descriptive_statistics_winsorized_pattern_length = udf {
      winsorizedValuesLength: mutable.Seq[Int] => {

        val valsAsDouble: mutable.Seq[Double] = winsorizedValuesLength.map(_.toDouble)
        val values: DenseVector[Double] = DenseVector[Double](valsAsDouble: _*)
        val mv = meanAndVariance(values)
        Map("winsorized-mean" -> mv.mean,
          "winsorized-std-dev" -> mv.stdDev)
      }
    }

    val statCol = "all-statistics"
    val trimmedStatCol = "trimmed-statistics"
    val winsorizedStatCol = "winsorized-statistics"
    val statisticsAboutPatternLenght: DataFrame = newFeaturesWithPatternStatsDF
      .withColumn(statCol, descriptive_statistics_pattern_length(col("pattern-length")))
      .withColumn("pattern-length-min", col(statCol).getItem("min-length"))
      .withColumn("pattern-length-max", col(statCol).getItem("max-length"))
      .withColumn("mean-pattern-length", col(statCol).getItem("mean"))
      .withColumn("variance-pattern-length", col(statCol).getItem("var"))
      .withColumn("std-dev-pattern-length", col(statCol).getItem("stddev"))
      .withColumn("most-freq-pattern-lenght", col(statCol).getItem("most-freq-pattern-length"))
      .withColumn("lower-quartile-pattern-length", col(statCol).getItem("lower-quartile"))
      .withColumn("upper-quartile-pattern-length", col(statCol).getItem("upper-quartile"))
      .withColumn("mad-of-length-distr", col(statCol).getItem("mad"))
      .withColumn("median-length-distr", col(statCol).getItem("median"))
      .withColumn(trimmedStatCol, descriptive_statistics_trimmed_pattern_length(col("pattern-length-trimmed")))
      .withColumn("trimmed-mean-pattern-length", col(trimmedStatCol).getItem("trimmed-mean"))
      .withColumn("trimmed-std-dev-pattern-length", col(trimmedStatCol).getItem("trimmed-std-dev"))
      .withColumn(winsorizedStatCol, descriptive_statistics_winsorized_pattern_length(col("pattern-length-winsorized")))
      .withColumn("winsorized-mean-pattern-length", col(winsorizedStatCol).getItem("winsorized-mean"))
      .withColumn("winsorized-std-dev-pattern-length", col(winsorizedStatCol).getItem("winsorized-std-dev"))
      .drop(statCol)
      .drop(trimmedStatCol)
      .drop(winsorizedStatCol)

    statisticsAboutPatternLenght
  }


}

object MetadataCreator {
  def apply(): MetadataCreator = new MetadataCreator()
}
