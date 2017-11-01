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

        /**
          * +-----+------+-------------------+---------------+------------------+---------------------+-------------+
          * |RecID|attrNr|value              |final-predictor|attribute         |newvalue-1           |newvalue-2   |
          * +-----+------+-------------------+---------------+------------------+---------------------+-------------+
          *
          **/


        val errorsAndReparsDF: DataFrame = predictedErrorsDF
          .select(FullResult.recid, FullResult.attrnr, FullResult.value, "final-predictor")
          .join(fullRepairResultsFilledDF, SchemaUtil.joinCols)

        errorsAndReparsDF.show(300, false)

        /**
          *
          * +------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
          * |attrNr|clean-values-set                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
          * +------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
          * |6     |[D14, 76, C-39, C24, C20, G10, A2G, G18, K13, G14A, 3, 62, 45, A19, C16, 42, 22, 102, 73, 84, D21, 75, C18, B42, G100, 75A, A29, 126, A35, B29, B38, D19, 29, B6, 1, 32, 69A, 11, B34, G12, 83, A20, D1, C21, 31, 113, 68, C36, D71, 23, C31, D44, 70, G3, C37, 27, CHK, D79, A3, B22, A37, A8, 94, B28, C14, C39, 70A, K9, 17, E35, 135, B9, 92, D37, B35, G15, G17, G6B, D22, B11, 33, 12, C10, H15, G14, G1A, B32, C19, C5, A5, D23, 5, 87A, 8, A1A, 16, D29, D3, B8, B45, B36, 132, D73, D24, D4, 80, C7, 78B, C6, C17, B24, 125, K7, A21, B46, 34, 6, A38, C15, D45, 75B, 15, G98, B16, 81, D7, B18, 82, 6A, 9, 123, 38, 76B, C22, 133, 137, C2, 68A, 41, 21, A22, H4, D17, C23, 77, A18, G7, E37, 43, A28, 60, D39, 88, C9, A1G, 134, 86, C27, G16, A6, G9, 107, 39, 95, 2, C25, 35, 16?, D47, G92, C12, 112, G2B, B44, D5, C-44, 64, G5, 87, C11, G11, -, A34, A24, B33, D27, D32, A36, C26, C28, K15, D16, C3, C29, 128, 72, 37, 131, D75, P1, A39, D20, H3A, D40, G19, A2C, D28, D25, 96, D18, 136, A7, 70B, D77, L8, 71, G19A, 44, B20, D8, D38, G6A, C8, D33, B14, D36, D10, C4, A4, G94, 26, D41, 98, E36, D12, 6B, 10, D34, E5, 20, 40, G96, B26]|
          * +------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
          *
          *
          **/


        val cleanValSetColumn = "clean-values-set"
        val aggregatedCleanValuesPerAttr: DataFrame = errorsAndReparsDF
          .filter(col("final-predictor") === 0.0)
          .groupBy(FullResult.attrnr)
          .agg(collect_set(FullResult.value) as cleanValSetColumn)

        aggregatedCleanValuesPerAttr.show(false)

        /**
          *
          * +------+-----+-----+---------------+---------+-----------+-----------------------------+----------------+
          * |attrNr|RecID|value|final-predictor|attribute|newvalue-1 |newvalue-2                   |clean-values-set|
          * +------+-----+-----+---------------+---------+-----------+-----------------------------+----------------+
          *
          **/

        var errorsAndProposedSolutions = errorsAndReparsDF
          .join(aggregatedCleanValuesPerAttr, Seq(FullResult.attrnr), "full_outer")
        errorsAndProposedSolutions = errorsAndProposedSolutions
          .na
          .fill("#NO-CLEAN-SET#", Seq(cleanValSetColumn))

        errorsAndProposedSolutions.show(300, false)



        //todo: nadeef deduplication. extend every class with method: public Collection<Fix> repair(Violation violation) {
        //todo: run nadeef deduplication repair.

        //todo: combine all of them into one
      }
    }
  }

}
