package de.evaluation.data.cleaning.results

import com.typesafe.config.ConfigFactory
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
import org.apache.spark.sql.DataFrame

object CleaningResultsCollector extends ExperimentsCommonConfig {

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

        val noCleanSetPlaceholder = "#NO-CLEAN-SET#"
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

        errorsAndProposedSolutions.printSchema()
        errorsAndProposedSolutions.show(30, false)

        //        val appConf = ConfigFactory.load("experiments.conf")
        //        errorsAndProposedSolutions
        //          .coalesce(1)
        //          .write
        //          .option("header", "True")
        //          .csv(appConf.getString(s"$dataset.repair.folder"))


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

        dsFDs.foreach(fd => {
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

          // rhsRepair.show(false)

          dfAttributesOnly
            //.where(col(FullResult.attrnr).isin(lhsIndx: _*) || col(FullResult.attrnr).isin(rhsIndx: _*))
            //.where(col("final-predictor") === 1.0)
            .join(rhsRepair, dfAttributesOnly(FullResult.value) === rhsRepair(s"lhs-${FullResult.value}"), "full_outer")

            .show(false)


        })



        //todo: Missing values:
        //todo: Missing values -> should be investigated in more detail, because not all missing value should be imputed;
        //todo: How to decide what column is repairable.

        //todo: Outliers:
        //todo: if the value is marked as an outlier. (eg.Selected by dBoost)
        //todo: use their values distribution method - histograms to determine the mean value (most probable repair)
        //todo: distinguish between discrete and continuous values

        //todo: Conbinatorial Multiarmed Bandits;


        //todo: nadeef deduplication. extend every class with method: public Collection<Fix> repair(Violation violation) {
        //todo: run nadeef deduplication repair.

        //todo: combine all of them into one
      }
    }
  }

}
