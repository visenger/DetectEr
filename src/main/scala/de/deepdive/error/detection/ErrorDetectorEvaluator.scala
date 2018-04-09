package de.deepdive.error.detection

import com.typesafe.config.ConfigFactory
import de.deepdive.error.detection.PredicateErrorCreator.allSchemasByName
import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.holoclean.HospPredictedSchema
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

  val pathToExtDict = "/Users/visenger/research/datasets/zip-code/free-zipcode-database-Primary.csv"

  val targetPath = "/Users/visenger/deepdive_notebooks/error_detection"


  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("Create Predicates") {
      session => {

        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)
          .where(col(FullResult.attrnr) === "7")

        val indicator = "indicator"

        //val errorDetectResultPath = s"$targetPath/result/error-detect.csv"
        val errorDetectResultPath = s"$targetPath/result/error-detect_1.csv"
        val resultDF: DataFrame = DataSetCreator
          .createFrame(session, errorDetectResultPath, Seq(FullResult.recid, FullResult.attrnr, FullResult.value, indicator, "expectation"): _*)
          .where(col(FullResult.attrnr) === "7")

        resultDF.show()

        val joinedDF: DataFrame = predictedMatrixDF
          .join(resultDF, Seq(FullResult.recid, FullResult.attrnr))
          .select(predictedMatrixDF(FullResult.recid),
            predictedMatrixDF(FullResult.attrnr),
            predictedMatrixDF(FullResult.value),
            resultDF(indicator),
            predictedMatrixDF(FullResult.label),
            resultDF("expectation"))

        val tp: Long = joinedDF
          .where(predictedMatrixDF(FullResult.label) === "1.0" && resultDF(indicator) === "1").count()

        val fp: Long = joinedDF
          .where(predictedMatrixDF(FullResult.label) === "0.0" && resultDF(indicator) === "1").count()

        val totalErrors: Long = joinedDF
          .where(predictedMatrixDF(FullResult.label) === "1.0").count()

        val precision: Double = tp / (tp + fp).toDouble
        val recall: Double = tp / totalErrors.toDouble

        val f1: Double = 2 * precision * recall / (precision + recall)

        println(s"evaluating only FD1: Zip=>State: eval performs on State attribute")
        println(s"Precision: ${NumbersUtil.round(precision, 4)}, Recall: ${NumbersUtil.round(recall, 4)}, F-1: ${NumbersUtil.round(f1, 4)}")


      }
    }
  }
}
