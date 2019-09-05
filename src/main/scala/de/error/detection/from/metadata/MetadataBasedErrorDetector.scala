package de.error.detection.from.metadata

import com.typesafe.config.{Config, ConfigFactory}
import de.aggregation.{MajorityVotingAggregator, UnionAllAggregator}
import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.{BeersSchema, FlightsSchema, Schema}
import de.evaluation.f1.{Eval, F1}
import de.evaluation.util.{MemoryEstimator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.{FormatUtil, NumbersUtil}
import de.util.DatasetFlattener
import de.util.ErrorNotation._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable


trait ConfigBase {

  val applicationConfig: Config = ConfigFactory.load()

  val commonsConfig: Config = ConfigFactory.load("data-commons.conf")
  val VALID_NUMBER = commonsConfig.getString("valid.number")
  val VALID_SSN = commonsConfig.getString("valid.ssn")
  val VALID_ZIP_1 = commonsConfig.getString("valid.zip1")
  val VALID_ZIP_2 = commonsConfig.getString("valid.zip2")
  val VALID_STATE_1 = commonsConfig.getString("valid.state1")
  val VALID_STATE_2 = commonsConfig.getString("valid.state2")

  val strDefaultValues: Seq[String] = commonsConfig.getStringList("default.string").asScala
  val intDefaultValues: Seq[String] = commonsConfig.getStringList("default.integer").asScala
  val dateDefaultValues: Seq[String] = commonsConfig.getStringList("default.date").asScala


  val ec_valid_number = "ec-valid-number"
  val ec_low_counts = "ec-low-counts"
  val ec_top_value = "ec-top-value"
}


/**
  * prototype to an error detection based on metadata information;
  */
object MetadataBasedErrorDetector extends ExperimentsCommonConfig with ConfigBase {

  def getDefaultsByDataType(dataType: String): Seq[String] = {
    dataType match {
      case "String" => strDefaultValues
      case "Integer" => intDefaultValues
      case "Date/Time" => dateDefaultValues
      //todo: extend with more defaults
      case _ => intDefaultValues
    }
  }

  def getContainsInDefaults(defaultValues: Seq[String], value: String): Int = {
    val containsInDefaults: Boolean = defaultValues.map(_.toLowerCase()).contains(value.toLowerCase())

    containsInDefaults match {
      case true => ERROR
      case false => CLEAN
    }
  }

  def getCoverageForRule(dataDF: DataFrame, column: String): Double = {

    val ruleImpactCount: Long = dataDF.where(col(column) =!= DOES_NOT_APPLY).count()
    val total: Long = dataDF.count()

    val coveragePercent: Double = ruleImpactCount.toDouble * 100 / total.toDouble
    val coveragePercentRounded: Double = NumbersUtil.round(coveragePercent, scale = 4)

    coveragePercentRounded
  }

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("metadata-based heuristics") {
      session => {

        Seq("museum"/*, "beers", "flights" , "blackoak"*/).foreach(dataset => {
          println(s"processing $dataset.....")


          val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
          val fullMetadataDF: DataFrame = creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
          // fullMetadataDF.show(50)

          val flatWithLabelDF: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)

          val flatWithMetadataDF: DataFrame = flatWithLabelDF.join(fullMetadataDF, Seq("attrName"))

          //                    Timer.measureRuntime {
          //                      () => {
          //                        println(s"computing metadata for $dataset")
          //                        creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
          //                        DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)
          //                      }
          //                    }


          //Error Classifier #1: missing values -> object UDF


          //Error Classifier #2: default values
          val schema: Schema = allSchemasByName.getOrElse(dataset, FlightsSchema)
          val dataTypesDictionary: Map[String, String] = schema.dataTypesDictionary

          def identify_default_values = udf {
            (attrName: String, value: String) => {

              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "")

                dataType match {
                  case "String" => result = getContainsInDefaults(strDefaultValues, value)
                  case "Integer" => result = getContainsInDefaults(intDefaultValues, value)
                  case "Date/Time" => result = getContainsInDefaults(dateDefaultValues, value)
                  case _ => result = DOES_NOT_APPLY
                }
              }
              result
            }
          }

          //Error classifier #3: top-values (aka histogram)
          def is_top_value = udf {
            (value: String, attrName: String, top10Values: mutable.Seq[String]) => {

              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                // remove default values from the top10
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "")
                val defaults: Seq[String] = getDefaultsByDataType(dataType).map(_.toLowerCase())
                val topValsWithoutDefaults: mutable.Seq[String] = top10Values.map(_.toLowerCase()).diff(defaults)

                val valueInTop10: Boolean = topValsWithoutDefaults.contains(value.toLowerCase())
                valueInTop10 match {
                  case true => result = CLEAN
                  case false => result = ERROR
                }
              }
              result
            }
          }

          //Error Classifier #4: Semantic role for numbers
          def isValidNumber(value: String): Int = {

            val allDigits: Boolean = value.matches(VALID_NUMBER)
            allDigits match {
              case true => CLEAN
              case false => ERROR
            }
          }

          def is_valid_number = udf {
            (attrName: String, value: String) => {
              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                //todo: remove default values from the top10
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "")

                dataType match {
                  case "Integer" => result = isValidNumber(value)
                  case _ => result = DOES_NOT_APPLY
                }
              }
              result
            }
          }

          //Error Classifier #5: cardinality estimation for PK columns
          val allTuplesViolatingCardinality: List[String] = CardinalityEstimator()
            .dataSetName(dataset)
            .onSchema(schema)
            .forDirtyDataDF(dirtyDF)
            .forFullMetadataDF(fullMetadataDF)
            .getTuplesViolatedCardinality()

          // println(s"the number of tuples violating cardinality constraint: ${allTuplesViolatingCardinality.size}")

          //Error Classifier #6: The column, related to the lookup source, its value is absent in the source -> error, else clean.
          //for unrelated columns -> 0

          val lookupsLoader: LookupsLoader = LookupsLoader()
            .dataSetName(dataset)
            .onSchema(schema)
            .forDirtyDataDF(dirtyDF)

          val lookupCols: Seq[String] = lookupsLoader.getLookupColumns()
          val lookupsByAttr: Map[String, Seq[String]] = lookupsLoader
            .load(session)

          def is_valid_by_lookup = udf {
            (attrName: String, recId: String) => {
              if (lookupCols.isEmpty || !lookupCols.contains(attrName)) DOES_NOT_APPLY
              else {
                if (lookupsByAttr(attrName).contains(recId)) ERROR
                else CLEAN

              }
            }
          }
          //end: Lookups


          //TODO: Not integrated due to performance issues on flights:
          // Error Classifier # values with low probabilities are suspicious

          def is_value_with_low_counts = udf {
            (numOfTuples: Long, columnDistinctVals: Int, value: String, valuesWithCounts: Map[String, Int]) => {

              var result = DOES_NOT_APPLY
              //todo: ValuesWithCounts are not optimal for flights

              if (numOfTuples == columnDistinctVals) result = DOES_NOT_APPLY
              else {
                if (valuesWithCounts.contains(value)) {
                  val counts: Int = valuesWithCounts.getOrElse(value, 0)
                  result = if (counts > 1) CLEAN else ERROR
                } else result = DOES_NOT_APPLY
              }
              result
            }
          }

          //Error Classifier: 6 # misfielded values
          //analysing the pattern length distribution and selecting the trimmed distribution
          //if the value is inside then clean otherwise error
          //if number of distinct pattern length <=3 then does-not-apply
          /**
            *
            * param value        :String the cell value
            * param valuesLength : Seq[Int] the set of the trimmed distribution (threshold 10%) of values pattern length
            */
          def is_value_pattern_length_within_trimmed_distr = udf {
            (value: String, valuesLength: Seq[Int]) => {
              var result = DOES_NOT_APPLY

              if (value == null) result = DOES_NOT_APPLY
              else {
                result = if (valuesLength.contains(value.size)) DOES_NOT_APPLY //here: we cannot say anything about the CLEAN
                else ERROR
              }


              result
            }
          }

          //end:#6 misfielded values

          //Error Classifier #7 validate data types by utilizing regular expressions to validate data against the type.

          def is_valid_data_type = udf {
            (attrName: String, value: String) => {
              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "").toLowerCase
                result = dataType match {
                  case "social security number" =>
                    //if (value.matches(VALID_SSN)) CLEAN else ERROR //todo: shall be this. Hack!
                    if (value.matches(VALID_NUMBER)) CLEAN else ERROR
                  case "zip code" => if (value.matches(VALID_ZIP_1) || value.matches(VALID_ZIP_2)) CLEAN else ERROR
                  case "state" => if (value.toLowerCase.matches(VALID_STATE_1) || value.toLowerCase.matches(VALID_STATE_2)) CLEAN else ERROR
                  case _ => DOES_NOT_APPLY
                }
              }
              result
            }
          }


          //end: #7 valid data type

          // Error classifier: Unused columns, indicated either by being largely unpopulated or populated with the same value in all records.
          def is_column_unused = udf {
            distinct_vals_count: Int => {
              if (distinct_vals_count <= 1) ERROR
              else DOES_NOT_APPLY
            }
          }

          //Error classifier: String length outliers:
          // Value to String to String-Length -> create length statistics with median, lower/upper quartlile
          // heuristic: lower_quartile > = len(value) >= upper_quartile -> ERROR


          /**
            * STRING LENGTH STATISTICS
            * Minimum
            * 4.00
            * Lower Quartile
            * 7.00
            * Median
            * 8.00
            * Upper Quartile
            * 9.00
            * Maximum
            * 26.00
            * Average
            * 7.84
            * Standard Deviation
            * 1.80
            */

          //Laurikkala et al. suggest a heuristic of 1.5*inter-quartile range
          //Reference: Laurikkala, J., Juhola, M. & Kentala, E. (2000). Informal Identification of Outliers in
          //Medical Data. Fifth International Workshop on Intelligent Data Analysis in Medicine
          //and Pharmacology IDAMAP-2000 Berlin, 22 August.

          def is_value_len_within_common_sizes = udf {
            (value: String, lowQuartile: Double, upQuartile: Double) => {
              var result = DOES_NOT_APPLY
              if (value != null && value.nonEmpty) {
                val len: Int = value.length
                if (len < lowQuartile || len > upQuartile) result = ERROR
              }
              result
            }
          }

          // Error classifier: Hampel x84 outliers detection (see Hellerstein survey paper)

          def is_value_outlier_Hampelx84 = udf {
            (value: String, mad: Double, median: Double) => {
              var result = DOES_NOT_APPLY
              if (value != null && value.nonEmpty) {
                val len: Int = value.length
                val lowerBound: Double = median - (mad * 1.4826)
                val upperBound: Double = median + (mad * 1.4826)
                if (len < lowerBound || len > upperBound) result = ERROR
              }
              result
            }
          }


          //Error classifier: extreme value theory (EVT) which uses a
          //Gaussian mixture model to represent the data distribution for outlier
          //detection as outliers (extreme values) occur in the tails of the distributions
          //P(extreme-val)=exp(-exp((-1)*((val - mean)/deviation))
          //from: A Survey of Outlier Detection Methodologies
          //VICTORIA J. HODGE & JIM AUSTIN
          // https://link.springer.com/content/pdf/10.1023/B:AIRE.0000045502.10941.a9.pdf


          def is_value_len_extreme_evt = udf {
            (value: String, mean: Double, std_dev: Double) => {
              var result = DOES_NOT_APPLY
              val isValidValue: Boolean = value != null && value.nonEmpty
              val stdDevIsValid: Boolean = std_dev != 0.0
              if (isValidValue && stdDevIsValid) {
                val len: Int = value.length
                // EVT (extreme value theory)
                val reduced_variate: Double = (len - mean) / std_dev
                val innerEVT: Double = math.exp((-1) * reduced_variate)
                val evt: Double = math.exp((-1) * innerEVT)
                val threshold = 0.9 //0.95
                if (evt > threshold) result = ERROR
              }
              result
            }
          }

          //Error classifier: 1.5 IQR
          // Laurikkala et al. suggest a heuristic of 1.5*inter-quartile range
          //Reference: Laurikkala, J., Juhola, M. & Kentala, E. (2000). Informal Identification of Outliers in
          //Medical Data. Fifth International Workshop on Intelligent Data Analysis in Medicine
          //and Pharmacology IDAMAP-2000 Berlin, 22 August.

          def is_value_len_within_1_5_iqr = udf {
            (value: String, lowQuartile: Double, upQuartile: Double) => {
              var result = DOES_NOT_APPLY
              if (value != null && value.nonEmpty) {
                val len: Int = value.length
                if (len < 1.5 * lowQuartile || len > 1.5 * upQuartile) result = ERROR
              }
              result
            }
          }

          //Error classifier: Z-value test: if z>=3 then error

          def is_value_len_z_test = udf {
            (value: String, mean: Double, std_dev: Double) => {
              var result = DOES_NOT_APPLY
              val isValidValue: Boolean = value != null && value.nonEmpty
              val stdDevIsValid: Boolean = std_dev != 0.0
              if (isValidValue && stdDevIsValid) {
                val len: Int = value.length
                //  z-value test
                val z_value: Double = math.abs(len - mean) / std_dev
                result = if (z_value >= 3) ERROR else DOES_NOT_APPLY

              }
              result
            }
          }

          // Error classifier: Value length is within trimmed or winsorized range [mean-2*stdDev; mean+2*stdDev]

          def is_value_len_within_range = udf {
            (value: String, mean: Double, std_dev: Double, factor: Double) => {
              var result = DOES_NOT_APPLY
              val isValidValue: Boolean = value != null && value.nonEmpty
              val stdDevIsValid: Boolean = std_dev != 0.0
              if (isValidValue && stdDevIsValid) {
                val len: Int = value.length
                val lowerBound: Double = mean - factor * std_dev
                val upperBound: Double = mean + factor * std_dev

                result = if (len < lowerBound || len > upperBound) ERROR else DOES_NOT_APPLY

              }
              result
            }
          }

          //Error classifier: Pattern value regex: https://cloud.trifacta.com/data/7259/22536
          // see https://docs.trifacta.com/display/SS/Text+Matching to create patterns
          // create patterns histogram and analyse for errors

          //Error Classifier: Missing value: Strings with repeated characters or characters that are next
          //to each other on the used keyboard, e.g., replacing a phone
          //number with 5555555555.

          // Error Classifier: detect disguised missing values that are far from the rest of the values in the Euclidean
          //space


          //Error Classifier # Supported data types
          //https://docs.trifacta.com/display/PE/Supported+Data+Types

          //Error Classifier # Spell checker for the text attributes

          //Error Classifier # For inter-column dependencies; see Fan book for the SQL-based solution for error detection
          /**
            * Deepdive format
            * #FD1: zip -> state
            * error_candidate(t1, 6, s1, 1):-
            * initvalue(t1, 7, z1),
            * initvalue(t2, 7, z2),
            * initvalue(t1, 6, s1),
            * initvalue(t2, 6, s2),
            * [t1!=t2, z1=z2, s1!=s2].
            */


          //todo:
          // Error classifier: M−3⁎MAD < x< M+3⁎MAD, where M is median and MAD is median absolute deviation
          //source: Leys, C., et al., "Detecting outliers: Do not use standard deviation around the mean,
          // use absolute deviation around the median",
          // Journal of Experimental Social Psychology (2013)
          //
          /** * ################ Final Matrix ##################
            */
          //gathering matrix with all error classifier results
          val ec_missing_value = "ec-missing-value"
          val ec_default_value = "ec-default-value"
          val ec_top_value = "ec-top-value"
          val ec_valid_number = "ec-valid-number"
          val ec_cardinality_vio = "ec-cardinality-vio"
          val ec_lookup = "ec-lookup-attr"
          //val ec_low_counts = "ec-low-counts-suspicious"
          val ec_pattern_length_within_trimmed_dist = "ec-pattern-value"
          val ec_valid_data_type = "ec-valid-data-type"
          val ec_unused_column = "ec-unused-column"
          val ec_value_len_within_common_sizes = "ec-value-len"
          val ec_value_len_Hampelx84 = "ec-value-len-Hampelx84"
          val ec_value_len_evt = "ec-value-len-evt"
          val ec_value_len_1_5_IQR = "ec-value-len-1-5-IQR"
          val ec_value_len_z_test = "ec-value-len-z-test"
          val ec_value_len_within_trimmed_range = "ec-value-len-trimmed-range"
          val ec_value_len_within_winsorized_range = "ec-value-len-winsorized-range"


          val matrixWithECsFromMetadataDF: DataFrame = flatWithMetadataDF
            .withColumn(ec_missing_value, UDF.identify_missing(flatWithMetadataDF("dirty-value").isNull))
            .withColumn(ec_default_value,
              identify_default_values(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .withColumn(ec_top_value, is_top_value(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("attrName"), flatWithMetadataDF("top10")))
            .withColumn(ec_valid_number, is_valid_number(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .withColumn(ec_cardinality_vio,
              when(flatWithMetadataDF(schema.getRecID).isin(allTuplesViolatingCardinality: _*), lit(ERROR))
                .otherwise(lit(DOES_NOT_APPLY)))
            .withColumn(ec_lookup, is_valid_by_lookup(flatWithMetadataDF("attrName"), flatWithMetadataDF(schema.getRecID)))
            /*.withColumn(ec_low_counts,
              is_value_with_low_counts(flatWithMetadataDF("number of tuples"),
                flatWithMetadataDF("distinct-vals-count"),
                flatWithMetadataDF("dirty-value"),
                flatWithMetadataDF("values-with-counts")))*/
            .withColumn(ec_valid_data_type, is_valid_data_type(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .withColumn(ec_unused_column, is_column_unused(flatWithMetadataDF("distinct-vals-count")))
            .withColumn(ec_pattern_length_within_trimmed_dist,
              is_value_pattern_length_within_trimmed_distr(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("pattern-length-dist-10")))
            .withColumn(ec_value_len_within_common_sizes, is_value_len_within_common_sizes(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("lower-quartile-pattern-length"), flatWithMetadataDF("upper-quartile-pattern-length")))
            .withColumn(ec_value_len_Hampelx84, is_value_outlier_Hampelx84(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("mad-of-length-distr"), flatWithMetadataDF("median-length-distr")))
            .withColumn(ec_value_len_evt, is_value_len_extreme_evt(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("mean-pattern-length"), flatWithMetadataDF("std-dev-pattern-length")))
            .withColumn(ec_value_len_1_5_IQR, is_value_len_within_1_5_iqr(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("lower-quartile-pattern-length"), flatWithMetadataDF("upper-quartile-pattern-length")))
            .withColumn(ec_value_len_z_test, is_value_len_z_test(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("mean-pattern-length"), flatWithMetadataDF("std-dev-pattern-length")))
            .withColumn(ec_value_len_within_trimmed_range, is_value_len_within_range(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("trimmed-mean-pattern-length"), flatWithMetadataDF("trimmed-std-dev-pattern-length"), lit(3.0)))
            .withColumn(ec_value_len_within_winsorized_range, is_value_len_within_range(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("winsorized-mean-pattern-length"), flatWithMetadataDF("winsorized-std-dev-pattern-length"), lit(3.0)))
          /*End: Final matrix*/

          matrixWithECsFromMetadataDF.show(5)



          val cols = Seq(
            ec_missing_value,
            ec_default_value,
            ec_top_value,
            ec_valid_number,
            ec_cardinality_vio,
            ec_lookup,
            ec_valid_data_type,
            ec_unused_column,
            //ec_pattern_length_within_trimmed_dist,
            ec_value_len_within_common_sizes,
            ec_value_len_Hampelx84,
            ec_value_len_evt,
            ec_value_len_1_5_IQR,
            ec_value_len_z_test,
            ec_value_len_within_trimmed_range,
            ec_value_len_within_winsorized_range)


          //coverageForMUSEUM(matrixWithECsFromMetadataDF, cols)

          //coverage for BEERS
          //coverageForBEERS(matrixWithECsFromMetadataDF, cols)

          //end: coveragae for BEERS


          /*End: Errors coverage by heuristics*/

          /* Analysing the performance of each classifier.*/
          //          val matrixWithClassifiersResult: DataFrame = matrixWithECsFromMetadataDF
          //            .select("label", cols: _*)
          // val countsByValues: collection.Map[Row, Long] = matrixWithClassifiersResult.rdd.countByValue()
          //          countsByValues.foreach(println)

          //          cols.foreach(column => {
          //            val singleECPerformnaceDF: DataFrame = FormatUtil
          //              .getPredictionAndLabelOnIntegersForSingleClassifier(matrixWithECsFromMetadataDF, column)
          //
          //            val evalEC: Eval = F1.evalPredictionAndLabels_TMP(singleECPerformnaceDF)
          //            evalEC.printResult(s"evaluation $column")
          //            //evalEC.printLatexString(s"$column")
          //          })

          //          cols.foreach(column => {
          //            val coverage: Double = getCoverageForRule(matrixWithECsFromMetadataDF, column)
          //            println(s"rule: $column, coverage: $coverage")
          //          })


          //                    val homeDir: String = applicationConfig.getString(s"home.dir.$dataset")
          //
          //
          //          cols.foreach(column => {
          //            val performanceDF: DataFrame = matrixWithECsFromMetadataDF
          //              .where(col(column) === ERROR)
          //              .withColumn("data-point", concat_ws("-", col(schema.getRecID), col("attrName")))
          //              .select("data-point")
          //              .toDF()
          //
          //            val pathToWrite = s"$homeDir/ec-classiefiers-performance/$dataset-$column-perfomance"
          //            WriterUtil.persistCSVWithoutHeader(performanceDF, pathToWrite)
          //
          //          })
          /* End: Analysing the performance of each classifier*/

          /* create Matrix for persistence */

          //          val matrixCols = Seq("attrName",
          //            "label",
          //            ec_missing_value,
          //            ec_default_value,
          //            ec_top_value,
          //            ec_valid_number,
          //            ec_cardinality_vio,
          //            ec_lookup,
          //            ec_valid_data_type,
          //            ec_unused_column,
          //            ec_pattern_length_within_trimmed_dist,
          //            ec_value_len_within_common_sizes,
          //            ec_value_len_Hampelx84,
          //            ec_value_len_evt,
          //            ec_value_len_1_5_IQR,
          //            ec_value_len_z_test,
          //            ec_value_len_within_trimmed_range,
          //            ec_value_len_within_winsorized_range)
          //
          //          val matrixWithClassifiersResult: DataFrame = matrixWithECsFromMetadataDF
          //            .select(schema.getRecID, matrixCols: _*)
          //
          //          val homeDir: String = applicationConfig.getString(s"home.dir.$dataset")
          //
          //          WriterUtil.persistCSV(matrixWithClassifiersResult, s"$homeDir/ec-matrix")

          /* end: matrix for persistence */

          /* Create matrix for the Dawid-Skene model to discover true item states/effects from multiple noisy measurements
          * for details see here: http://aclweb.org/anthology/Q14-1025 */
          //          val classifierCols = Seq(ec_missing_value,
          //            ec_default_value,
          //            ec_top_value,
          //            ec_valid_number,
          //            ec_cardinality_vio,
          //            ec_lookup,
          //            ec_pattern_length_within_trimmed_dist,
          //            ec_valid_data_type)
          //
          //
          //          val dawidSkeneModelDF: DataFrame = DawidSkeneModel()
          //            .onDataset(dataset)
          //            .onDataFrame(matrixWithECsFromMetadataDF)
          //            .onColumns(classifierCols)
          //            .createModel()
          //
          //
          //          val homeDir: String = applicationConfig.getString(s"home.dir.$dataset")
          //          WriterUtil.persistCSV(dawidSkeneModelDF, s"$homeDir/dawidSkeneModel-matrix")
          /* end: matrix for the Dawid-Skene model */


          /* Aggregation strategies */
          //          println(s"aggregated columns: ${cols.mkString(",")}")
          //          val unionAllAggregator: UnionAllAggregator = UnionAllAggregator()
          //            .onDataFrame(matrixWithECsFromMetadataDF).forColumns(cols)
          //          val unionAllEval: Eval = unionAllAggregator.evaluate()
          //          unionAllEval.printResult(s"union all for $dataset: ")
          //          unionAllEval.printLatexString(s"union all for $dataset: ")

          //          val majorityVotingAggregator: MajorityVotingAggregator = MajorityVotingAggregator()
          //            .onDataFrame(matrixWithECsFromMetadataDF).forColumns(cols)
          //          val majorityVoteEval: Eval = majorityVotingAggregator.evaluate()
          //          majorityVoteEval.printResult(s"majority vote for $dataset")
          //          majorityVoteEval.printLatexString(s"majority vote for $dataset")
          /* end: Aggregation strategies */

        })


      }
    }
  }

  private def coverageForBEERS(matrixWithECsFromMetadataDF: DataFrame, cols: Seq[String]): Unit = {
    println("MISFIELDED, WRONG DATA TYPE, MISSING, ILLEGAL VALUES")
    cols.foreach(ec => {

      /*MISFIELDED*/
      //col(city), col(state)
      val city = "city"
      val state = "state"
      val missfieldedDF: DataFrame = matrixWithECsFromMetadataDF
        .where(col("attrName") === city or col("attrName") === state)
        .where(col("label") === 1).toDF()
      val totalMisfielded: Long = missfieldedDF.count()

      val coverageByECMisf: Long = missfieldedDF.where(col(ec) === 1).count()

      val misfielded: Double = NumbersUtil.percentageFound(totalMisfielded.toDouble, coverageByECMisf.toDouble)

      //println(s" heuristic $ec coverage of misfielded values = $misfielded")

      /*WRONG DATA TYPE*/
      val abv = "abv"

      val wrongDataTypeDF: DataFrame = matrixWithECsFromMetadataDF.where(col("attrName") === abv)
        .where(col("label") === 1).toDF()

      val totalWrongDtype: Long = wrongDataTypeDF.count()
      val coverageByECWDT: Long = wrongDataTypeDF.where(col(ec) === 1).count()
      val wrongDataType: Double = NumbersUtil.percentageFound(totalWrongDtype.toDouble, coverageByECWDT.toDouble)

      /*DEFAULT_VALUE*/
      /*MISSING*/
      val ibu = "ibu"

      val missingDefaultDF: DataFrame = matrixWithECsFromMetadataDF.where(col("attrName") === ibu)
        .where(col("label") === 1).toDF()
      val totalMissingDefault: Long = missingDefaultDF.count()

      val coverageByECMD: Long = missingDefaultDF.where(col(ec) === 1).count()
      val missingDefault: Double = NumbersUtil.percentageFound(totalMissingDefault.toDouble, coverageByECMD.toDouble)


      /*ILLEGAL VALUES*/
      val ounces = "ounces"
      val illegalValsDF: DataFrame = matrixWithECsFromMetadataDF.where(col("attrName") === ounces)
        .where(col("label") === 1).toDF()

      val totalIllegalVals: Long = illegalValsDF.count()

      val coverageByECIV: Long = illegalValsDF.where(col(ec) === 1).count()
      val illegal: Double = NumbersUtil.percentageFound(totalIllegalVals.toDouble, coverageByECIV.toDouble)

      println(s"heuristic $ec & $misfielded & $wrongDataType & $missingDefault & $illegal")


    })
  }

  private def coverageForMUSEUM(matrixWithECsFromMetadataDF: DataFrame, cols: Seq[String]): Unit = {
    /**
      * Errors coverage by heuristics for MUSEUM
      **/

    val errorTypesMuseum = Seq(
      "#LV_AMBIGUOUS_DATA#",
      "#LV_MISFIELDED#",
      "#LV_UNUSED#",
      "#LV_MISSING#",
      "#LV_EXTRANEOUS_DATA#",
      "#LV_DEFAULT_VALUE#",
      "#LV_DIFFERENT_WORD_ORDERING#")


    cols.foreach(ec => {

      var jointCoverage: mutable.Seq[Double] = mutable.Seq()

      errorTypesMuseum.foreach(errorType => {

        val errorsCount: Long = matrixWithECsFromMetadataDF
          .where(col("clean-value") === errorType)
          .count()

        val coverageByEC: Long = matrixWithECsFromMetadataDF
          .where(col(ec) === 1 and col("clean-value") === errorType)
          .count()

        jointCoverage = jointCoverage :+ NumbersUtil
          .percentageFound(errorsCount.toDouble, coverageByEC.toDouble)

        println(s"heuristic name: $ec, coverage of the $errorType = $coverageByEC")
      })


      println(s"heuristic $ec & ${jointCoverage.mkString(" & ")}")
    })
  }
}

/**
  * Noting when values recognizable as belonging to multiple data domains appear in one column
  */
object MissfieldedValuesErrorDetector extends ExperimentsCommonConfig with ConfigBase {
  def main(args: Array[String]): Unit = {

    val datasets = Seq("beers_missfielded_1", "beers_missfielded_5", "beers_missfielded_10")

    SparkLOAN.withSparkSession("missfielded errors detector") {
      session => {
        datasets.foreach(dataset => {

          println(s"processing $dataset.....")

          val schema: Schema = allSchemasByName.getOrElse(dataset, BeersSchema)

          val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
          val fullMetadataDF: DataFrame = creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
          //fullMetadataDF.show()

          val flatWithLabelDF: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)

          val flatWithMetadataDF: DataFrame = flatWithLabelDF.join(fullMetadataDF, Seq("attrName"))
          // flatWithMetadataDF.where(col("label") === "1").show()


          //Error Classifier #6: The column, related to the lookup source, its value is absent in the source -> error, else clean.
          //for unrelated columns -> 0

          val lookupsLoader: LookupsLoader = LookupsLoader()
            .dataSetName("beers")
            .onSchema(schema)
            .forDirtyDataDF(dirtyDF)

          val lookupCols: Seq[String] = lookupsLoader.getLookupColumns()
          val lookupsByAttr: Map[String, Seq[String]] = lookupsLoader
            .load(session)

          def is_valid_by_lookup = udf {
            (attrName: String, recId: String) => {
              if (lookupCols.isEmpty || !lookupCols.contains(attrName)) DOES_NOT_APPLY
              else {
                if (lookupsByAttr(attrName).contains(recId)) ERROR
                else CLEAN

              }
            }
          }
          //end: Lookups

          val ec_pattern_length_within_trimmed_dist = "ec-pattern-value"
          val ec_low_counts = "ec-low-counts-suspicious"
          val ec_lookup = "ec-lookup-attr"

          val matrixWithECsFromMetadataDF: DataFrame = flatWithMetadataDF
            .where(col("attrName") === "city") //todo: perform analysis on one column
            .withColumn(ec_pattern_length_within_trimmed_dist,
            UDF.is_value_pattern_length_within_trimmed_distr(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("pattern-length-dist-10")))
            .withColumn(ec_low_counts,
              UDF.is_value_with_low_counts(flatWithMetadataDF("number of tuples"),
                flatWithMetadataDF("distinct-vals-count"),
                flatWithMetadataDF("dirty-value"),
                flatWithMetadataDF("values-with-counts")))
            .withColumn(ec_lookup, is_valid_by_lookup(flatWithMetadataDF("attrName"), flatWithMetadataDF(schema.getRecID)))


          val ec_columns = Seq(ec_pattern_length_within_trimmed_dist,
            ec_low_counts,
            ec_lookup)

          ec_columns.foreach(column => {
            val singleECPerformnaceDF: DataFrame = FormatUtil
              .getPredictionAndLabelOnIntegersForSingleClassifier(matrixWithECsFromMetadataDF, column)
            val evalEC: Eval = F1.evalPredictionAndLabels_TMP(singleECPerformnaceDF)
            evalEC.printResult(s"evaluation $column")

          })

          /* Aggregation strategies */

          /*//1-st aggregation: majority voting*/

          val majorityVoterEval: Eval = MajorityVotingAggregator()
            .onDataFrame(matrixWithECsFromMetadataDF)
            .forColumns(ec_columns)
            .evaluate()
          majorityVoterEval.printResult(s"majority voter for $dataset:")


          /*//2-nd aggregation: min-1*/

          val unionAllEval: Eval = UnionAllAggregator()
            .onDataFrame(matrixWithECsFromMetadataDF)
            .forColumns(ec_columns)
            .evaluate()
          unionAllEval.printResult(s"min-1 for $dataset")

          /* end: Aggregation strategies */

        })
      }
    }

  }
}

/**
  * noting the violation of the data type constraint
  */
object WrongdatatypeErrorDetector extends ExperimentsCommonConfig with ConfigBase {

  def main(args: Array[String]): Unit = {

    val datasets = Seq("beers_wrongdatatype_1", "beers_wrongdatatype_5", "beers_wrongdatatype_10")

    SparkLOAN.withSparkSession("wrong data type errors detector") {
      session => {
        datasets.foreach(dataset => {

          println(s"processing $dataset.....")

          val schema: Schema = allSchemasByName.getOrElse(dataset, BeersSchema)

          val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
          val fullMetadataDF: DataFrame = creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
          // fullMetadataDF.show()

          val flatWithLabelDF: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)

          val flatWithMetadataDF: DataFrame = flatWithLabelDF.join(fullMetadataDF, Seq("attrName"))
          // flatWithMetadataDF.where(col("label") === "1").show()

          //Error Classifier #4: Semantic role for numbers

          val dataTypesDictionary: Map[String, String] = schema.dataTypesDictionary

          def isValidNumber(value: String): Int = {

            val allDigits: Boolean = value.matches(VALID_NUMBER)
            allDigits match {
              case true => CLEAN
              case false => ERROR
            }
          }

          def is_valid_number = udf {
            (attrName: String, value: String) => {
              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                //todo: remove default values from the top10
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "")

                dataType match {
                  case "Integer" => result = isValidNumber(value)
                  case _ => result = DOES_NOT_APPLY
                }
              }
              result
            }
          }


          val matrixWithECsFromMetadataDF: DataFrame = flatWithMetadataDF
            .where(col("attrName") === "ounces" or col("attrName") === "abv")
            .withColumn(ec_valid_number, is_valid_number(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .withColumn(ec_low_counts,
              UDF.is_value_with_low_counts(flatWithMetadataDF("number of tuples"),
                flatWithMetadataDF("distinct-vals-count"),
                flatWithMetadataDF("dirty-value"),
                flatWithMetadataDF("values-with-counts")))

          val ec_columns: Seq[String] = Seq(ec_valid_number, ec_low_counts)
          ec_columns.foreach(column => {
            val singleECPerformnaceDF: DataFrame = FormatUtil
              .getPredictionAndLabelOnIntegersForSingleClassifier(matrixWithECsFromMetadataDF, column)
            val evalEC: Eval = F1.evalPredictionAndLabels_TMP(singleECPerformnaceDF)
            evalEC.printResult(s"evaluation $column")
          })

          val unionAllEval: Eval = UnionAllAggregator()
            .onDataFrame(matrixWithECsFromMetadataDF)
            .forColumns(ec_columns)
            .evaluate()
          unionAllEval.printResult(s"union all for $dataset: ")

        })
      }
    }
  }


}


object MisspellingErrorDetector extends ExperimentsCommonConfig with ConfigBase {

  def main(args: Array[String]): Unit = {
    val datasets = Seq("beers_misspelings_1", "beers_misspelings_5", "beers_misspelings_10")

    SparkLOAN.withSparkSession("misspelling errors detector") {
      session => {
        datasets.foreach(dataset => {

          println(s"processing $dataset.....")

          val schema: Schema = allSchemasByName.getOrElse(dataset, BeersSchema)

          val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
          val fullMetadataDF: DataFrame = creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
          //fullMetadataDF.show()

          val flatWithLabelDF: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)

          val flatWithMetadataDF: DataFrame = flatWithLabelDF.join(fullMetadataDF, Seq("attrName"))
          //flatWithMetadataDF.where(col("label") === "1").show()

          val dataTypesDictionary: Map[String, String] = schema.dataTypesDictionary

          def isValidNumber(value: String): Int = {

            val allDigits: Boolean = value.matches(VALID_NUMBER)
            allDigits match {
              case true => CLEAN
              case false => ERROR
            }
          }

          def is_valid_number = udf {
            (attrName: String, value: String) => {
              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                //todo: remove default values from the top10
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "")

                dataType match {
                  case "Integer" => result = isValidNumber(value)
                  case _ => result = DOES_NOT_APPLY
                }
              }
              result
            }
          }


          val matrixWithECsFromMetadataDF: DataFrame = flatWithMetadataDF
            .withColumn(ec_top_value, UDF.is_in_top_10_value(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("attrName"), flatWithMetadataDF("top10")))
            .withColumn(ec_valid_number, is_valid_number(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .withColumn(ec_low_counts,
              UDF.is_value_with_low_counts(flatWithMetadataDF("number of tuples"),
                flatWithMetadataDF("distinct-vals-count"),
                flatWithMetadataDF("dirty-value"),
                flatWithMetadataDF("values-with-counts")))


          val ec_columns = Seq(ec_top_value, ec_valid_number, ec_low_counts)
          ec_columns.foreach(column => {
            val singleECPerformnaceDF: DataFrame = FormatUtil
              .getPredictionAndLabelOnIntegersForSingleClassifier(matrixWithECsFromMetadataDF, column)
            val evalEC: Eval = F1.evalPredictionAndLabels_TMP(singleECPerformnaceDF)
            evalEC.printResult(s"evaluation $column")

          })

          val unionAllEval: Eval = UnionAllAggregator()
            .onDataFrame(matrixWithECsFromMetadataDF).forColumns(ec_columns)
            .evaluate()
          unionAllEval.printResult(s"union all for $dataset: ")

          val majorityVoteEval: Eval = MajorityVotingAggregator()
            .onDataFrame(matrixWithECsFromMetadataDF).forColumns(ec_columns)
            .evaluate()

          majorityVoteEval.printResult(s"majority vote for $dataset")

        })

      }
    }
  }
}


