package de.experiments.holoclean

import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.NumbersUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

class HolocleanEvaluator {

}

object Evaluator {


  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("EVALUATOR") {
      session => {

        //        val groundtruthPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/groundtruth.csv"
        //        val dirtyInputPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/input_data.csv"
        //
        //        val cleaningResultPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/holoclean-output.csv"
        //        println("holoclean hosp: no pruning, no error detection")
        //        evaluate(session, groundtruthPath, cleaningResultPath, dirtyInputPath)
        //
        //        val idealErrorDetection = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/holoclean-output-ideal-error-detection.csv"
        //        println("holoclean hosp: prunning done by perfect error detection")
        //        evaluate(session, groundtruthPath, idealErrorDetection, dirtyInputPath)


        println("OUR HOSP - NO PRUNING:")

        // todo : value predicate always union with the clean values.

        val ourDataGroundtruthPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/our-hosp/groundtruth/hosp-groundtruth.csv"
        val ourDirtyDataPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/our-hosp/dirty/hosp-dirty-input.csv"

        val ourCleaningResultPerfectPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/our-hosp/deepdive-result/deepdive-result-ideal-error-detection.csv"
        println("our hosp: perfect error detection")
        evaluate(session, ourDataGroundtruthPath, ourCleaningResultPerfectPath, ourDirtyDataPath)

        val ourCleaningResultWithErrorDetPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/our-hosp/deepdive-result/deepdive_result_custom_error_detection.csv"
        println("our hosp: domain on error detection result: f1 50% ")
        evaluate(session, ourDataGroundtruthPath, ourCleaningResultWithErrorDetPath, ourDirtyDataPath)

        println("OUR HOSP - WITH PRUNING (domain on error detection result: f1 50%)")
        Seq(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0).foreach(t => {
          val ourCleaningResultWithErrorDetPath = s"/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/our-hosp/deepdive-result/deepdive_result_custom_err_detect_${t}_pruning.csv"
          println(s"our hosp: prunning threshold: $t ")
          evaluate(session, ourDataGroundtruthPath, ourCleaningResultWithErrorDetPath, ourDirtyDataPath)

        })


      }
    }
  }

  def evaluate(session: SparkSession, groundtruth: String, cleaningResult: String, dirtyInput: String): Unit = {

    val schema = Seq("ind", "attr", "val")
    val cleaningSchema = Seq("ind", "attr", "val", "expectation")

    val groundtruthDF: DataFrame = DataSetCreator.createFrame(session, groundtruth, schema: _*)
    val cleaningResultDF: DataFrame = DataSetCreator
      .createFrame(session, cleaningResult, cleaningSchema: _*)
      .select("ind", "attr", "val")
    val dirtyInputDF: DataFrame = DataSetCreator.createFrame(session, dirtyInput, schema: _*)

    val incorrect: DataFrame = cleaningResultDF.except(groundtruthDF).toDF()
    val errors: DataFrame = dirtyInputDF.except(groundtruthDF).toDF()
    val uncorrected: DataFrame = errors.intersect(incorrect).toDF()
    //todo:
    val incorrectValues: Long = incorrect.count()
    val repair: Long = cleaningResultDF.count()
    val uncorrectedValues: Long = uncorrected.count()

    val precision: Double = (repair - incorrectValues) / repair.toDouble
    val recall: Double = 1.0 - (uncorrectedValues / errors.count().toDouble)

    val f1: Double = (2.0 * precision * recall) / (precision + recall)

    println(s"Precision: ${NumbersUtil.round(precision, 4)}, Recall: ${NumbersUtil.round(recall, 4)}, F-1: ${NumbersUtil.round(f1, 4)}")

  }
}
