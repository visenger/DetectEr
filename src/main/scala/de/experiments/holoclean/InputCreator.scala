package de.experiments.holoclean

import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.generation.FeaturesGenerator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

class InputCreator {

}

object HospPredictedSchema {

  val attr1 = "missingValue"
  val attr2 = "isTop10"
  val attr3 = "inTail"
  val attr4 = "String-type"
  val attr5 = "Boolean-type"
  val attr6 = "Zip Code-type"
  val attr7 = "Phone Number-type"
  val attr8 = "LHS-zip -> city,  state"
  val attr9 = "LHS-zip, address -> phone"
  val attr10 = "LHS-city, address -> phone"
  val attr11 = "LHS-state, address -> phone"
  val attr12 = "LHS-prno, mc -> stateavg"
  val attr13 = "RHS-zip -> city, state"
  val attr14 = "RHS-zip, address -> phone"
  val attr15 = "RHS-city, address -> phone"
  val attr16 = "RHS-state, address -> phone"
  val attr17 = "RHS-prno, mc -> stateavg"
  val attr18 = "exists-1"
  val attr19 = "exists-2"
  val attr20 = "exists-3"
  val attr21 = "exists-4"
  val attr22 = "exists-5"
  val attr23 = "RecID"
  val attr24 = "attrNr"
  val attr25 = "value"
  val attr26 = "label"
  val attr27 = "final-predictor"

  val schema = Seq(attr1,
    attr2,
    attr3,
    attr4,
    attr5,
    attr6,
    attr7,
    attr8,
    attr9,
    attr10,
    attr11,
    attr12,
    attr13,
    attr14,
    attr15,
    attr16,
    attr17,
    attr18,
    attr19,
    attr20,
    attr21,
    attr22,
    attr23,
    attr24,
    attr25,
    attr26,
    attr27)
}

/**
  * Creates a sample from the dataset.
  */
object HospSampleGenerator {
  val dataset = "hosp"
  val pathToPrediction = "/Users/visenger/research/datasets/clean-and-dirty-data/HOSP-10K/matrix-with-prediction/hosp-with-prediction.csv"

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("Dataset sample") {
      session => {
        val generator = new FeaturesGenerator()
        val dirtyDataDF: DataFrame = generator.onDatasetName(dataset).getDirtyData(session)

        val Array(smallDirtyDF, _) = dirtyDataDF.randomSplit(Array(0.1, 0.9))
        val rows: Array[Row] = smallDirtyDF.select(HospSchema.getRecID).collect()
        val dirtyIdsFromSample: List[String] = rows.map(row => row.getString(0)).toList

        println(s" number of id-elements from the hosp sample: ${dirtyIdsFromSample.size}")

        val hospWithErrorsPredictionDF: DataFrame = DataSetCreator.createFrame(session, pathToPrediction, HospPredictedSchema.schema: _*)

        val samplePredictedDF: DataFrame = hospWithErrorsPredictionDF
          .where(col(FullResult.recid).isin(dirtyIdsFromSample: _*)).toDF()
        // samplePredictedDF.printSchema() //todo: all values are strings, convert if needed

        //        val convert_to_double = udf {
        //          value: String => value.toDouble
        //        }
        //
        //        val statPrecitionLabel: collection.Map[(String, String), Long] = samplePredictedDF.select(FullResult.label, "final-predictor").rdd.map(row => {
        //          val label = row.getString(0)
        //          val prediction = row.getString(1)
        //          (prediction, label)
        //        }).countByValue()
        //
        //        println(statPrecitionLabel)
        //
        //        var tp = 0.0
        //        var fn = 0.0
        //        var tn = 0.0
        //        var fp = 0.0
        //
        //        statPrecitionLabel.foreach(elem => {
        //          val values = elem._1
        //          val count = elem._2
        //          values match {
        //            //confusion matrix
        //            //(prediction, label)
        //            case ("1.0", "1.0") => tp = count
        //            case ("1.0", "0.0") => fp = count
        //            case ("0.0", "1.0") => fn = count
        //            case ("0.0", "0.0") => tn = count
        //          }
        //        })
        //
        //        //    println(s"true positives: $tp")
        //
        //        val totalData = tp + fp + tn + fn
        //
        //        val accuracy = (tp + tn) / totalData.toDouble
        //        //    println(s"Accuracy: $accuracy")
        //        val precision = tp / (tp + fp).toDouble
        //        println(s"Precision: $precision")
        //
        //        val recall = tp / (tp + fn).toDouble
        //        println(s"Recall: $recall")
        //
        //        val F1 = 2 * precision * recall / (precision + recall)
        //        println(s"F-1 Score: $F1")

        val pathToData = "/Users/visenger/deepdive_notebooks/hosp-cleaning"

        smallDirtyDF
          .repartition(1)
          .write
          .option("header", "true")
          .csv(s"$pathToData/dirty-data")

        samplePredictedDF
          .repartition(1)
          .write
          .option("header", "true")
          .csv(s"$pathToData/predicted-data")


      }
    }
  }
}

/**
  * Automatic generating the evidence (observed) predicates.
  */
object EvidenceCreator extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    val dataset = "hosp"
    val pathToData = "/Users/visenger/deepdive_notebooks/hosp-cleaning"
    val dirtyDataPath = s"$pathToData/dirty-data/hosp-dirty-1k.csv"
    val matrixWithPredictionPath = s"$pathToData/predicted-data/hosp-1k-predicted-errors.csv"
    val schema: Schema = allSchemasByName.getOrElse(dataset, HospSchema)

    SparkLOAN.withSparkSession("Evidence Predicates") {
      session => {

        //Tuple
        val dirtyDF: DataFrame = DataSetCreator.createFrame(session, dirtyDataPath, schema.getSchema: _*)

        val tupleDF: DataFrame = dirtyDF.select(schema.getRecID)

        //InitValue
        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)
        val initValueDF: DataFrame = predictedMatrixDF
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)

        //Domain
        val domainByAttrDF: DataFrame = initValueDF
          .groupBy(col(FullResult.attrnr))
          .agg(collect_set(FullResult.value) as "domain")


        /**
          * using org.apache.spark.sql.functions#explode(org.apache.spark.sql.Column)
          * to flatten the values list.
          */
        val domainDF: DataFrame = initValueDF
          .join(domainByAttrDF, Seq(FullResult.attrnr), "full_outer")
          .select(col(FullResult.recid), col(FullResult.attrnr), explode(col("domain")).as("domain"))

        //HasFeature
        /**
          * first aggregate on RecID, second for each tuple in aggregated bin,
          * concatenate its values as "attrNr=value" -> feature
          */
        val featuresByRowIdDF: DataFrame = initValueDF
          .groupBy(col(FullResult.recid))
          .agg(collect_list(concat(col(FullResult.attrnr), lit("="), col(FullResult.value))) as "features")

        val hasFeatureDF: DataFrame = initValueDF
          .join(featuresByRowIdDF, Seq(FullResult.recid), "full_outer")
          .select(col(FullResult.recid), col(FullResult.attrnr), col(FullResult.value), explode(col("features")).as("feature"))

        //todo: persist all

      }
    }


  }
}







