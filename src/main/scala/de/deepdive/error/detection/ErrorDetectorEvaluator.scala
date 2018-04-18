package de.deepdive.error.detection

import com.typesafe.config.ConfigFactory
import de.deepdive.error.detection.PredicateErrorCreator.allSchemasByName
import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.holoclean.HospPredictedSchema
import de.model.util.NumbersUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ErrorDetectorEvaluator {

}

object EvaluatorDetectEr {
  val dataset = "hosp"
  val config = ConfigFactory.load()
  val cleanDataPath = config.getString(s"data.$dataset.clean.10k")
  val pathToData = "/Users/visenger/deepdive_notebooks/hosp-cleaning"
  val dirtyDataPath = s"$pathToData/dirty-data/hosp-dirty-1k.csv"
  val matrixWithPredictionPath = s"$pathToData/predicted-data/hosp-1k-predicted-errors.csv"
  val schema: Schema = allSchemasByName.getOrElse(dataset, HospSchema)

  val groundtruthPath = "/Users/visenger/deepdive_notebooks/hosp-cleaning/groundtruth/hosp-groundtruth.csv"


  val targetPath = "/Users/visenger/deepdive_notebooks/error_detection"


  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("Create Predicates") {
      session => {

        //val state = "6" //our encoding
        val finalPredictor = "final-predictor"

        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)

        //  predictedMatrixDF.where(col(FullResult.label) === "1.0").show()


        /**
          *
          * the result is obtained by running the query:
          *
          * SELECT
          * tid,
          * attr,
          * value,
          * CASE WHEN label = TRUE
          * THEN 1.0
          * ELSE 0.0 END as prediction
          * FROM error;
          */


        val errorDetectPath = s"$targetPath/result/error_detection_10.csv"

        val prediction = "prediction"
        val errDetectionDF: DataFrame = DataSetCreator
          .createFrame(session, errorDetectPath,
            Seq(FullResult.recid, FullResult.attrnr, FullResult.value, prediction): _*)

        val counts: collection.Map[(Double, Double), Long] = predictedMatrixDF
          .join(errDetectionDF, Seq(FullResult.recid, FullResult.attrnr))
          .select(col(prediction), col(FullResult.label))
          .rdd
          .map(row => (row.getString(0).toDouble, row.getString(1).toDouble))
          .countByValue()

        var truePos: Long = 0
        var trueNeg: Long = 0
        var falsePos: Long = 0
        var falseNeg: Long = 0

        counts.foreach(entry => {
          val count: Long = entry._2
          val pair: (Double, Double) = entry._1
          pair match {
            //(prediction, label)
            case (1.0, 1.0) => truePos = count
            case (1.0, 0.0) => falsePos = count
            case (0.0, 1.0) => falseNeg = count
            case (0.0, 0.0) => trueNeg = count
          }
        })

        val precision: Double = computePrecision(truePos, falsePos)
        val totalErrors: Long = truePos + falseNeg
        val recall: Double = computeRecall(truePos, totalErrors)
        val f1: Double = computeF1(precision, recall)

        println(s"evaluating: $errorDetectPath")
        println(s"tp: $truePos, fp: $falsePos, total errors: ${totalErrors}")
        println(s"Precision: ${NumbersUtil.round(precision, 4)}, Recall: ${NumbersUtil.round(recall, 4)}, F-1: ${NumbersUtil.round(f1, 4)}")


        println()
        println("Baseline eval: our error detection")
        val tp1: Long = predictedMatrixDF
          .where(predictedMatrixDF(finalPredictor) === "1.0"
            && predictedMatrixDF(FullResult.label) === "1.0")
          .count()

        val fp1: Long = predictedMatrixDF
          .where(predictedMatrixDF(finalPredictor) === "1.0"
            && predictedMatrixDF(FullResult.label) === "0.0")
          .count()

        val precision1: Double = computePrecision(tp1, fp1)
        val recall1: Double = computeRecall(tp1, totalErrors)


        val f1Baseline: Double = computeF1(precision1, recall1)

        println(s"baseline tp: $tp1, fp: $fp1, total errors: $totalErrors")
        println(s"Precision: ${NumbersUtil.round(precision1, 4)}, Recall: ${NumbersUtil.round(recall1, 4)}, F-1: ${NumbersUtil.round(f1Baseline, 4)}")


      }
    }
  }

  private def computeRecall(tp: Long, totalErrors: Long): Double = {
    tp / totalErrors.toDouble
  }

  private def computePrecision(tp: Long, fp: Long): Double = {
    tp / (tp + fp).toDouble
  }

  private def computeF1(precision: Double, recall: Double): Double = {
    2 * precision * recall / (precision + recall)
  }
}
