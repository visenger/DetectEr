package de.evaluation.data.cleaning.results

import de.evaluation.data.util.SchemaUtil
import de.evaluation.f1.FullResult
import de.evaluation.tools.pattern.violation.TrifactaResults
import de.evaluation.tools.ruleviolations.nadeef.NadeefRulesVioResults
import de.evaluation.util.SparkLOAN
import de.experiments.features.error.prediction.ErrorsPredictor
import org.apache.spark.sql.DataFrame

object CleaningResultsCollector {

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

        /*we first get all repairs together*/
        val fullRepairResultsFilledDF = fullRepairResultsDF
          .na
          .fill("#NO-REPAIR#", Seq(repairColFromRulesVio, repairColFromPattenVio))

        /*second, we aggregate errors (by stacking or bagging), delivered by several error recognition frameworks
        The value "final-predictor" is the result of the aggregation
        The attribute "VALUE" is the original value taken from the dirty dataset.*/
        val predictedErrorsDF: DataFrame = ErrorsPredictor()
          .onDataset(dataset)
          .runPredictionWithStacking(session)

        val errorsAndReparsDF: DataFrame = predictedErrorsDF
          .select(FullResult.recid, FullResult.attrnr, FullResult.value, "final-predictor")
          .join(fullRepairResultsFilledDF, SchemaUtil.joinCols)

        errorsAndReparsDF.show(300, false)



        //todo: finish -
        // 1) separate errors and clean values;
        // 2) group clean values and connect them to errors as possible solutions;

        val cleanValSetColumn = "clean-values-set"
        val aggregatedCleanValuesPerAttr: DataFrame = errorsAndReparsDF
          .filter(col("final-predictor") === 0.0)
          .groupBy(FullResult.attrnr)
          .agg(collect_set(FullResult.value) as cleanValSetColumn)

        aggregatedCleanValuesPerAttr.show(false)

        val errorsAndProposedSolutions = errorsAndReparsDF
          .join(aggregatedCleanValuesPerAttr, Seq(FullResult.attrnr), "full_outer")
          .na
          .fill("#NO-CLEAN-SET#", Seq(cleanValSetColumn))

        errorsAndProposedSolutions.show(300,false)


        //todo: nadeef deduplication. extend every class with method: public Collection<Fix> repair(Violation violation) {
        //todo: run nadeef deduplication repair.

        //todo: combine all of them into one
      }
    }
  }

}
