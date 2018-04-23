package de.deepdive.error.detection

import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.holoclean.HospPredictedSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class PredicatesForErrorDetection {

}

object PredicateErrorCreator extends ExperimentsCommonConfig {
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
        //Tuple
        val dirtyDF: DataFrame = DataSetCreator.createFrame(session, dirtyDataPath, schema.getSchema: _*)

        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)

        val tupleDF: DataFrame = dirtyDF.select(schema.getRecID)

        //        tupleDF
        //          .repartition(1)
        //          .write
        //          .option("header", "false")
        //          .csv(s"$targetPath/input/tuple")

        //InitValue
        val initValueDF: DataFrame = predictedMatrixDF
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)

        //        initValueDF
        //          .repartition(1)
        //          .write
        //          .option("sep", "\\t")
        //          .option("header", "false")
        //          .csv(s"$targetPath/input/initvalue")

        //Error(tid, attr, value, error_indicator)
        //error_indicator can be -1 or 1 reflecting the presence of an error (-1: false; 1: true).
        //val error1DF: DataFrame = initValueDF.withColumn("error-indicator", lit(1))
        val error0DF: DataFrame = initValueDF.withColumn("error-indicator", lit("\\N"))

        val errorCandidateDF: DataFrame = error0DF
          // .union(error1DF)
          .withColumn("label", lit("\\N"))
          .toDF()

        /*errorCandidateDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$targetPath/input/error_candidate")*/

        val errorDF: DataFrame = initValueDF
          .withColumn("label", lit("\\N"))
          .toDF()

        //        errorDF.repartition(1)
        //          .write
        //          .option("sep", "\\t")
        //          .option("header", "false")
        //          .csv(s"$targetPath/input/error")


        val inTail: DataFrame = predictedMatrixDF
          .where(col("inTail") === "1.0")
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)
        //        inTail
        //          .repartition(1)
        //          .write
        //          .option("sep", "\\t")
        //          .option("header", "false")
        //          .csv(s"$targetPath/input/intail")

        val isTop10DF: DataFrame = predictedMatrixDF
          .where(col("isTop10") === "1.0")
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)

        //        isTop10DF
        //          .repartition(1)
        //          .write
        //          .option("sep", "\\t")
        //          .option("header", "false")
        //          .csv(s"$targetPath/input/top_ten")

        val missingValueDF: DataFrame = predictedMatrixDF
          .where(col("missingValue") === "1.0")
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)

        //        missingValueDF
        //          .repartition(1)
        //          .write
        //          .option("sep", "\\t")
        //          .option("header", "false")
        //          .csv(s"$targetPath/input/missing_value")

        /**
          * using external knowledge bases/dictionaries to create predicates
          *
          */

        val (zipDF, cityDF, stateDF) = ExternalDictionariesPredicatesCreator
          .createZipCityStatesPredicates(session, pathToExtDict)

        /*zipDF.repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$targetPath/input/ext-zip")

        cityDF.repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$targetPath/input/ext-city")

        stateDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$targetPath/input/ext-state")*/


      }


    }
  }
}
