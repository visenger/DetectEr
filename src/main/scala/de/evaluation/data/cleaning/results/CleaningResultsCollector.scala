package de.evaluation.data.cleaning.results

import com.typesafe.config.ConfigFactory
import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.HospSchema
import de.evaluation.data.util.SchemaUtil
import de.evaluation.f1.FullResult
import de.evaluation.tools.pattern.violation.TrifactaResults
import de.evaluation.tools.ruleviolations.nadeef.NadeefRulesVioResults
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.error.prediction.ErrorsPredictor
import de.experiments.features.generation.FeaturesGenerator
import de.experiments.metadata.FD
import de.model.util.NumbersUtil
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object CleaningResultsCollector extends ExperimentsCommonConfig {

  def percentageFound(total: Long, foundByMethod: Long, msg: String): Unit = {
    val percent = NumbersUtil.round(((foundByMethod * 100) / total.toDouble), 4)
    println(s"$msg $percent %")
  }

  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("CLEAN-MODEL") {
      session => {
        import org.apache.spark.sql.functions._

        // val datasets = Seq(/*"blackoak", "hosp", "salaries",*/ "flights")
        val dataset = "flights"

        val rulesVioResults = new NadeefRulesVioResults()
        rulesVioResults.onData(dataset)
        val tmp_rulesVioResultDF: DataFrame = rulesVioResults.createRepairLog(session)
        val repairColFromRulesVio = "newvalue-1"
        val rulesVioResultDF = tmp_rulesVioResultDF.withColumnRenamed("newvalue", repairColFromRulesVio)

        val patternVioResults = new TrifactaResults()
        patternVioResults.onDataset(dataset)
        val tmp_patternVioResultDF: DataFrame = patternVioResults.createRepairLog(session)
        val repairColFromPattenVio = "newvalue-2"
        val patternVioResultDF = tmp_patternVioResultDF.withColumnRenamed("newvalue", repairColFromPattenVio)

        val allResults = Seq(rulesVioResultDF, patternVioResultDF)
        val fullRepairResultsDF: DataFrame = allResults.tail
          .foldLeft(allResults.head)((acc, tool) => acc.join(tool, SchemaUtil.joinCols, "full_outer"))

        val noRepairPlaceholder = "#NO-REPAIR#"
        /*we first get all repairs together*/
        val fullRepairResultsFilledDF = fullRepairResultsDF
          .na
          .fill(noRepairPlaceholder, Seq(repairColFromRulesVio, repairColFromPattenVio))


        /*second, we aggregate errors (by stacking or bagging), delivered by several error recognition frameworks
        The value "final-predictor" is the result of the aggregation
        The attribute "VALUE" is the original value taken from the dirty dataset.*/
        val predictedErrorsDF: DataFrame = ErrorsPredictor()
          .onDataset(dataset)
          .runPredictionWithStacking(session)
        //.runPredictionWithBagging(session)

        val errorsAndReparsDF: DataFrame = predictedErrorsDF
          //.select(FullResult.recid, FullResult.attrnr, FullResult.value, "final-predictor")
          .join(fullRepairResultsFilledDF, SchemaUtil.joinCols, "full_outer")


        //todo: persist errorsAndReparsDF for the POC-development, so we don't need the

        /**
          * +------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
          * |attrNr|clean-values-set                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
          * +------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
          * |6     |[D14, 76, C-39, C24, C20, G10, A2G, G18, K13, G14A, 3, 62, 45, A19, C16, 42, 22, 102, 73, 84, D21, 75, C18, B42, G100, 75A, A29, 126, A35, B29, B38, D19, 29, B6, 1, 32, 69A, 11, B34, G12, 83, A20, D1, C21, 31, 113, 68, C36, D71, 23, C31, D44, 70, G3, C37, 27, CHK, D79, A3, B22, A37, A8, 94, B28, C14, C39, 70A, K9, 17, E35, 135, B9, 92, D37, B35, G15, G17, G6B, D22, B11, 33, 12, C10, H15, G14, G1A, B32, C19, C5, A5, D23, 5, 87A, 8, A1A, 16, D29, D3, B8, B45, B36, 132, D73, D24, D4, 80, C7, 78B, C6, C17, B24, 125, K7, A21, B46, 34, 6, A38, C15, D45, 75B, 15, G98, B16, 81, D7, B18, 82, 6A, 9, 123, 38, 76B, C22, 133, 137, C2, 68A, 41, 21, A22, H4, D17, C23, 77, A18, G7, E37, 43, A28, 60, D39, 88, C9, A1G, 134, 86, C27, G16, A6, G9, 107, 39, 95, 2, C25, 35, 16?, D47, G92, C12, 112, G2B, B44, D5, C-44, 64, G5, 87, C11, G11, -, A34, A24, B33, D27, D32, A36, C26, C28, K15, D16, C3, C29, 128, 72, 37, 131, D75, P1, A39, D20, H3A, D40, G19, A2C, D28, D25, 96, D18, 136, A7, 70B, D77, L8, 71, G19A, 44, B20, D8, D38, G6A, C8, D33, B14, D36, D10, C4, A4, G94, 26, D41, 98, E36, D12, 6B, 10, D34, E5, 20, 40, G96, B26]|
          * +------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
          **/

        //val array_to_string = udf { array: mutable.WrappedArray[_] => array.mkString("|") }
        val cleanValSetColumn = "clean-values-set"
        val aggregatedCleanValuesPerAttr: DataFrame = errorsAndReparsDF
          .where(col("final-predictor") === 0.0)
          .groupBy(FullResult.attrnr)
          .agg(collect_set(FullResult.value) as cleanValSetColumn)
        //          .withColumn(s"$cleanValSetColumn-tmp", array_to_string(col(cleanValSetColumn)))
        //          .drop(cleanValSetColumn)
        //          .withColumnRenamed(s"$cleanValSetColumn-tmp", cleanValSetColumn)


        var errorsAndProposedSolutions = errorsAndReparsDF
          .join(aggregatedCleanValuesPerAttr, Seq(FullResult.attrnr), "full_outer")

        def empty_str_array = udf { () => Array.empty[String] }

        //val noCleanSetPlaceholder = "#NO-CLEAN-SET#"
        errorsAndProposedSolutions = errorsAndProposedSolutions
          .withColumn(s"$cleanValSetColumn-tmp",
            coalesce(col(cleanValSetColumn), empty_str_array()))
        // coalesce(col(cleanValSetColumn), lit(noCleanSetPlaceholder)))
        errorsAndProposedSolutions = errorsAndProposedSolutions
          .drop(cleanValSetColumn)
          .withColumnRenamed(s"$cleanValSetColumn-tmp", cleanValSetColumn)

        //todo: add true values to control the repair values

        val config = ConfigFactory.load()
        val cleanDataPath = config.getString(s"data.$dataset.clean")

        val datasetSchema = allSchemasByName.getOrElse(dataset, HospSchema)
        val schema: Seq[String] = datasetSchema.getSchema
        val cleanData: DataFrame = DataSetCreator.createFrame(session, cleanDataPath, schema: _*)
        val truthValue = "truth-value"

        val cleanAttributes: Seq[String] = schema.filterNot(_.equals(datasetSchema.getRecID))

        val attributesDFs = cleanAttributes.map(attribute => {
          val indexByAttrName = datasetSchema.getIndexesByAttrNames(List(attribute)).head
          val flattenDF = cleanData
            .select(datasetSchema.getRecID, attribute)
            .withColumn(FullResult.attrnr, lit(indexByAttrName))
            .withColumn(truthValue, cleanData(attribute))

          flattenDF
            .select(datasetSchema.getRecID, FullResult.attrnr, truthValue)
        })

        val cleanDF: DataFrame = attributesDFs
          .reduce((df1, df2) => df1.union(df2))
          .repartition(1)
          .toDF(FullResult.recid, FullResult.attrnr, truthValue)

        errorsAndProposedSolutions = errorsAndProposedSolutions
          .filter(col(FullResult.label).isNotNull)
          .join(cleanDF, SchemaUtil.joinCols)

        //        errorsAndProposedSolutions.printSchema()
        //        errorsAndProposedSolutions.show(30, false)


        //todo: use FDs to create refined data repair set:
        //todo: rules violation:
        //todo: get all fds
        //todo: get all rhs attributes and determine their attribute numbers
        //todo: get all lhs attributes and determine their attribute numbers
        //todo: create (lhs-idx, rhs-idx)
        //todo: for each rhs-idx where final-predictor is 1.0 get the RowID and get the set of lhs-values for this FD
        //todo: get the group of RowIDs where lhs equals the lhs of the previous step
        //todo: select all rhs values corresponding to the selected RowIDs


        val generator = FeaturesGenerator()
          .onDatasetName(dataset)

        val dsFDs: Seq[FD] = generator.allFDs

        val repairsDF: Seq[DataFrame] = dsFDs.map(fd => {
          val lhs: List[String] = fd.lhs
          val rhs: List[String] = fd.rhs

          val lhsIndx: List[Int] = datasetSchema.getIndexesByAttrNames(lhs)
          val rhsIndx: List[Int] = datasetSchema.getIndexesByAttrNames(rhs)

          var dfAttributesOnly: DataFrame = errorsAndProposedSolutions
            .where(col(FullResult.attrnr).isin(lhsIndx: _*) || col(FullResult.attrnr).isin(rhsIndx: _*))
            .toDF()

          var lhsAttributesRows = dfAttributesOnly
            .where(col(FullResult.attrnr).isin(lhsIndx: _*))
            .where(col("final-predictor") === 0.0)
            .withColumnRenamed(FullResult.value, s"lhs-${FullResult.value}")

          val rhsAttributesRows = dfAttributesOnly
            .where(col(FullResult.attrnr).isin(rhsIndx: _*))
            .where(col("final-predictor") === 0.0)
            .withColumnRenamed(FullResult.value, s"rhs-${FullResult.value}")

          val rhsRepair = lhsAttributesRows
            .join(rhsAttributesRows, FullResult.recid)
            .select(FullResult.recid, s"lhs-${FullResult.value}", s"rhs-${FullResult.value}")
            .groupBy(s"lhs-${FullResult.value}")
            .agg(collect_set(col(s"rhs-${FullResult.value}")) as s"fd-${fd.toString}-repair")

          val extendRhsRepairDF = dfAttributesOnly
            .where(col(FullResult.attrnr).isin(lhsIndx: _*))
            .join(rhsRepair, dfAttributesOnly(FullResult.value) === rhsRepair(s"lhs-${FullResult.value}"), "full_outer")

          val tmpRepairSetsDF = extendRhsRepairDF
            .filter(col(s"fd-${fd.toString}-repair").isNotNull)
            .select(col(FullResult.recid), col(s"fd-${fd.toString}-repair"))

          val rhsWithRepairSetDF = dfAttributesOnly
            .where(col(FullResult.attrnr).isin(rhsIndx: _*))
            .select(FullResult.recid, FullResult.attrnr)
            .join(tmpRepairSetsDF, Seq(FullResult.recid))
            .withColumnRenamed(s"fd-${fd.toString}-repair", s"fd-repair")
            .withColumn("fd-applied", lit(s"${fd.toString}"))

          rhsWithRepairSetDF
        })

        val fdRepairSetDF = repairsDF
          .reduce((df1, df2) => df1.union(df2))
          .repartition(1)
          .toDF(FullResult.recid, FullResult.attrnr, "fd-repair", "fd-applied")

        errorsAndProposedSolutions = errorsAndProposedSolutions
          .join(fdRepairSetDF, Seq(FullResult.recid, FullResult.attrnr), "full_outer")

        //todo: replace nulls with empty arrays or placeholder stings

        //        errorsAndProposedSolutions.printSchema()
        //        errorsAndProposedSolutions.show(31, false)

        //        todo: persist the results
        // todo: in order to persist, the array data types columns should be converted into the string version
        //        val appConf = ConfigFactory.load("experiments.conf")
        //        errorsAndProposedSolutions
        //          .coalesce(1)
        //          .write
        //          .option("header", "True")
        //          .csv(appConf.getString(s"$dataset.repair.folder"))


        //todo: Outliers:
        //todo: if the value is marked as an outlier. (eg.Selected by dBoost)
        //todo: use their values distribution method - histograms to determine the mean value (most probable repair)
        //todo: distinguish between discrete and continuous values


        val jsonPathForMetadata = allMetadataByName.getOrElse(dataset, "unknown")
        var metadataDF: DataFrame = MetadataCreator().getFullMetadata(session, jsonPathForMetadata)

        val get_attrNr_by_name = udf {
          name: String => datasetSchema.getIndexesByAttrNames(List(name)).head
        }

        metadataDF = metadataDF
          .where(col("attrName").isin(cleanAttributes: _*))
          .withColumn(s"meta-${FullResult.attrnr}", get_attrNr_by_name(col("attrName")))
          .select(col(s"meta-${FullResult.attrnr}"), col("histogram"))

        errorsAndProposedSolutions = errorsAndProposedSolutions
          .join(metadataDF,
            errorsAndProposedSolutions(FullResult.attrnr) === metadataDF(s"meta-${FullResult.attrnr}")
              && errorsAndProposedSolutions("final-predictor") === 1.0, "full_outer") /* we want to join the histogram vals on data marked as errors*/
          .drop(metadataDF(s"meta-${FullResult.attrnr}"))

        errorsAndProposedSolutions.printSchema()
        //        errorsAndProposedSolutions.show(32, false)

        //todo: get preliminary stats about the results:

        def contains_truth_value = udf {
          //todo: java.lang.NullPointerException if truth null is
          (truth: String, repair: String) => {

            truth.equalsIgnoreCase(repair) match {
              case true => 1.0
              case false => 0.0
            }
          }
        }

        def contains_in_list = udf {
          (truth: String, repair: mutable.Seq[String]) => {
            var result = 0.0

            val triedBoolean: Try[List[String]] = Try(repair.toList)
            triedBoolean match {
              case Success(_) => {
                repair.contains(truth) match {
                  case true => result = 1.0
                  case false => result = 0.0
                }
              }
              case Failure(_) => result = 0.0
            }

            result
          }
        }

        val nadeefResultCol = "newvalue-1"
        val trifactaResultCol = "newvalue-2"
        val histogram = "histogram"
        val fdRepair = "fd-repair"
        val preliminaryResults = errorsAndProposedSolutions
          .where(col("final-predictor") === 1.0) /* we care about predicted errors */
          .filter(col(truthValue).isNotNull) /* we don't need null values */
        // /* the code below just looks into suggested repairs sets*/
        //          .withColumn(s"contains-$nadeefResultCol",
        //          contains_truth_value(col(truthValue), col(nadeefResultCol)))
        //          .withColumn(s"contains-$trifactaResultCol",
        //            contains_truth_value(col(truthValue), col(trifactaResultCol)))
        //          .withColumn(s"contains-$histogram", contains_in_list(col(truthValue), col(histogram)))
        //          .withColumn(s"contains-$fdRepair", contains_in_list(col(truthValue), col(fdRepair)))
        //          .withColumn(s"contains-$cleanValSetColumn", contains_in_list(col(truthValue), col(cleanValSetColumn)))
        //          .select(col(truthValue),
        //            col(nadeefResultCol), col(s"contains-$nadeefResultCol"),
        //            col(trifactaResultCol), col(s"contains-$trifactaResultCol"),
        //            col(histogram), col(s"contains-$histogram"),
        //            col(fdRepair), col(s"contains-$fdRepair"),
        //            col(cleanValSetColumn), col(s"contains-$cleanValSetColumn"))

        //preliminaryResults.show()

        /*
        todo: start: preliminary results

        val total = preliminaryResults.count()
        val nadeefFound = preliminaryResults.where(col(s"contains-$nadeefResultCol") === 1.0).count()
        val trifactaFound = preliminaryResults.where(col(s"contains-$trifactaResultCol") === 1.0).count()
        val top10Found = preliminaryResults.where(col(s"contains-$histogram") === 1.0).count() //outliers
        val rulesFound = preliminaryResults.where(col(s"contains-$fdRepair") === 1.0).count()
        val allCleanValuesFound = preliminaryResults.where(col(s"contains-$cleanValSetColumn") === 1.0).count()

        def percentageFound(total: Long, foundByMethod: Long, msg: String): Unit = {
          val percent = NumbersUtil.round(((foundByMethod * 100) / total.toDouble), 4)
          println(s"$msg $percent %")
        }

        percentageFound(total, nadeefFound, "Nadeef repair")
        percentageFound(total, trifactaFound, "Trifacta repair")
        percentageFound(total, top10Found, "Zipf distribution repair")
        percentageFound(total, rulesFound, "FDs repair")
        percentageFound(total, allCleanValuesFound, "all clean values repair")

        val allRepairsCombinations = preliminaryResults.select(
          col(s"contains-$nadeefResultCol"),
          col(s"contains-$trifactaResultCol"),
          col(s"contains-$histogram"),
          col(s"contains-$fdRepair"),
          col(s"contains-$cleanValSetColumn")).rdd.map(row =>
          (row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3), row.getDouble(4))).countByValue()

        allRepairsCombinations.foreach(println(_))
        todo: end: preliminary results
*/


        //        newvalue-1: string (nullable = true)
        //        |-- newvalue-2: string (nullable = true)
        //        |-- clean-values-set: array (nullable = true)
        //        |    |-- element: string (containsNull = true)
        //        |-- fd-repair: array (nullable = true)
        //        |    |-- element: string (containsNull = true)
        //        |-- histogram: array (nullable = true)
        //        |    |-- element: string (containsNull = true)


        //todo: combining repair values sets
        def combine_repairs = udf {
          (repair1: String,
           repair2: String,
           allClean: mutable.Seq[String],
           fdClean: mutable.Seq[String],
           hist: mutable.Seq[String]) => {
            val totalNumberOfSets = 5
            val totalListOfRepair = Seq(Seq(repair1), Seq(repair2), allClean, fdClean, hist)
              .filter(_ != null) /* filter all null objects but Seq(null)'s are still there */
              .flatten /* flat all lists: Seq(null) becomes null*/
              .filter(_ != null)
            /* remove nulls */
            /* I <3 scala */
            val totalSet = totalListOfRepair.toSet

            val valuesWithProbs: Map[String, Double] = totalSet.map(element => {
              val numSetsIncludingElement: Int = totalListOfRepair.count(_.equalsIgnoreCase(element))
              val probOfElement = NumbersUtil.round(numSetsIncludingElement / totalNumberOfSets.toDouble, 4)
              element -> probOfElement
            }).toMap

            val mostFrequentElements: Seq[(String, Double)] = valuesWithProbs
              .toSeq
              .sortWith((pair1, pair2) => pair1._2 > pair2._2)
            val initProbability: Double = NumbersUtil.round(1 / totalNumberOfSets.toDouble, 4)
            val mostFrequentRepair: Seq[(String, Double)] = mostFrequentElements.filter(el_p => el_p._2 > initProbability)
            val endValues: Seq[String] = mostFrequentRepair.map(_._1)
            //todo: if endValues is empty, consider some default values set, e.g. non-empty fd's
            endValues
          }
        }

        val combinedRepairCol = "combined-repair"
        val combinedRepairs = preliminaryResults
          .withColumn(combinedRepairCol, combine_repairs(
            col(nadeefResultCol),
            col(trifactaResultCol),
            col(cleanValSetColumn),
            col(fdRepair),
            col(histogram)))

        val combiResultsWithContains = combinedRepairs
          .withColumn(s"contains-$combinedRepairCol", contains_in_list(col(truthValue), col(combinedRepairCol)))

        //        val total = combiResultsWithContains.count()
        //        val combiContainsTruth = combiResultsWithContains.where(col(s"contains-$combinedRepairCol") === 1.0).count()
        //        percentageFound(total, combiContainsTruth, "combined list")

        def suggest_repair = udf {
          allClean: mutable.Seq[String] => {
            allClean.headOption.getOrElse(noRepairPlaceholder)/* because allClean list could be an empty list*/
          }
        }

        val suggestedRepairCol = "suggested-repair"
        val suggestedRepair: DataFrame = combiResultsWithContains
          .withColumn(s"$suggestedRepairCol", suggest_repair(col(combinedRepairCol)))
          .withColumn(s"contains-$suggestedRepairCol", contains_truth_value(col(truthValue), col(suggestedRepairCol)))

        val total = suggestedRepair.count()
        val suggestedContainsTruth = suggestedRepair.where(col(s"contains-$suggestedRepairCol") === 1.0).count()
        percentageFound(total, suggestedContainsTruth, "suggested repair")

        suggestedRepair.show(45, false)


        //todo: Missing values:
        //todo: Missing values -> should be investigated in more detail, because not all missing value should be imputed;
        //todo: How to decide what column is repairable.


        //todo: Conbinatorial Multiarmed Bandits;

        //todo: nadeef deduplication. extend every class with method: public Collection<Fix> repair(Violation violation) {
        //todo: run nadeef deduplication repair.

        //todo: combine all of them into one
      }
    }
  }

}
