package de.experiments.holoclean

import java.util.Properties

import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.generation.FeaturesGenerator
import de.model.util.NumbersUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row}

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

object DeepdivePredictedSchema {
  val tid = "tid"
  val attr = "attr"
  val value = "value"
  val id = "id"
  val label = "label"
  val category = "category"
  val expectation = "expectation"

  val schema = Seq(tid, attr, value, id, label, category, expectation)
}

object ZipCodesDictSchema {
  val attr1 = "Zipcode"
  val attr2 = "ZipCodeType"
  val attr3 = "City"
  val attr4 = "State"
  val attr5 = "LocationType"
  val attr6 = "Lat"
  val attr7 = "Long"
  val attr8 = "Location"
  val attr9 = "Decommisioned"
  val attr10 = "TaxReturnsFiled"
  val attr11 = "EstimatedPopulation"
  val attr12 = "TotalWages"

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
    attr12)
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
object PredicatesCreator extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    val dataset = "hosp"
    val pathToData = "/Users/visenger/deepdive_notebooks/hosp-cleaning"
    val dirtyDataPath = s"$pathToData/dirty-data/hosp-dirty-1k.csv"
    val matrixWithPredictionPath = s"$pathToData/predicted-data/hosp-1k-predicted-errors.csv"
    val schema: Schema = allSchemasByName.getOrElse(dataset, HospSchema)

    val pathToExtDict = "/Users/visenger/research/datasets/zip-code/free-zipcode-database-Primary.csv"

    SparkLOAN.withSparkSession("Evidence Predicates") {
      session => {

        //Tuple
        val dirtyDF: DataFrame = DataSetCreator.createFrame(session, dirtyDataPath, schema.getSchema: _*)

        val tupleDF: DataFrame = dirtyDF.select(schema.getRecID)

        tupleDF
          .repartition(1)
          .write
          .option("header", "false")
          .csv(s"$pathToData/input/tuple")

        //InitValue
        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)
        val initValueDF: DataFrame = predictedMatrixDF
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)

        initValueDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/initvalue")


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

        domainDF.repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/domain")

        //Value?
        val valueDF: DataFrame = domainDF
          .withColumn("label", lit("\\N"))

        valueDF
          .repartition(1)
          .write.option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/value")


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
          .select(col(FullResult.recid), col(FullResult.attrnr), explode(col("features")).as("feature"))

        hasFeatureDF.repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/hasfeature")


        //ExtDict
        val zipCodesDF: DataFrame = DataSetCreator.createFrame(session, pathToExtDict, ZipCodesDictSchema.schema: _*)

        val extDictDF: DataFrame = zipCodesDF.select("Zipcode", "City", "State")
          .withColumn("RowId", monotonically_increasing_id())
          .withColumn("DictNumber", lit(1))
          .withColumn("valsToArray", array("Zipcode", "City", "State"))
          .select(col("RowId"), posexplode(col("valsToArray")).as(Seq("extAttrNr  ", "value")), col("DictNumber"))

        extDictDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/extdict")

        //Matched

        /**
          * The predicate *matched* has the following structure:
          * matched? (
          * tid bigint, -> from the dataset (RecId identifiers)
          * attr int, -> from the dataset (e.g state, city)
          * value text, -> from the ext dict
          * dict int -> dictionary identifier (counts in our case)
          * ).
          *
          */
        val allCitiesFromExtDict: List[String] = zipCodesDF
          .select("City")
          .distinct()
          .collect()
          .map(row => row.getString(0).trim).toList
        val citiesCols: List[Column] = allCitiesFromExtDict.map(city => lit(city))
        val cityIdx: Int = HospSchema.indexAttributes.getOrElse("city", 5)

        val allStatesFromExtDict: List[String] = zipCodesDF
          .select("State")
          .distinct()
          .collect()
          .map(row => row.getString(0).trim).toList
        val statesCols: List[Column] = allStatesFromExtDict.map(state => lit(state))
        val stateIdx: Int = HospSchema.indexAttributes.getOrElse("state", 6)

        val matchedCityDF: DataFrame = tupleDF
          .withColumn(FullResult.attrnr, lit(cityIdx))
          .withColumn("values", array(citiesCols: _*))
          .select(col(schema.getRecID), col(FullResult.attrnr), explode(col("values")).as("value"))

        val matchedStatesDF: DataFrame = tupleDF
          .withColumn(FullResult.attrnr, lit(stateIdx))
          .withColumn("values", array(statesCols: _*))
          .select(col(schema.getRecID), col(FullResult.attrnr), explode(col("values")).as("value"))

        val initialMatchedDF: DataFrame = matchedCityDF
          .union(matchedStatesDF)
          .toDF(FullResult.recid, FullResult.attrnr, "value")

        val matchedDF: DataFrame = initialMatchedDF
          .withColumn("dict", lit(1))
          .withColumn("label", lit("\\N"))

        matchedDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/matched")


      }
    }


  }
}


object EvaluateDeepdive extends ExperimentsCommonConfig {
  val dataset = "hosp"

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val cleanDataPath = config.getString(s"data.$dataset.clean.1k")
    val pathToData = "/Users/visenger/deepdive_notebooks/hosp-cleaning"
    val matrixWithPredictionPath = s"$pathToData/predicted-data/hosp-1k-predicted-errors.csv"
    //val deepdivePredictionPath = s"$pathToData/predicted-data/deepdive-predicted.csv"

    SparkLOAN.withSparkSession("EVAL DeepDive") {
      session => {

        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)


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

        val predictedErrorsWithTruthDF: DataFrame = predictedMatrixDF
          .join(cleanDF, Seq(FullResult.recid, FullResult.attrnr))
          .select(FullResult.recid, FullResult.attrnr, FullResult.value, FullResult.label, "final-predictor", "truth-value")

        val props: Properties = new Properties()
        props.put("user", "visenger")
        props.put("driver", "org.postgresql.Driver")

        val deep_dive_inference_result = "value_label_inference"
        val url = "jdbc:postgresql://localhost:5432/deepdive_hosp"

        val ddPredictionTable: DataFrame = session
          .read
          .jdbc(url, deep_dive_inference_result, props)
          .toDF(DeepdivePredictedSchema.schema: _*)

        /**
          * filter columns we care about: city and state (participated in integrity constraints)
          */
        val cityAttrNr = 5
        val stateAttrNr = 6

        val predicted_value = "predicted-value"
        val ddPrediction = ddPredictionTable
          .withColumnRenamed("value", predicted_value)
          .where(ddPredictionTable(DeepdivePredictedSchema.attr) === cityAttrNr
            or
            ddPredictionTable(DeepdivePredictedSchema.attr) === stateAttrNr)

        //        val ddPrediction: DataFrame = DataSetCreator
        //          .createFrame(session, deepdivePredictionPath, DeepdivePredictedSchema.schema: _*)

        val joinedPredictions: DataFrame = predictedErrorsWithTruthDF
          .join(ddPrediction,
            predictedErrorsWithTruthDF(FullResult.recid) === ddPrediction(DeepdivePredictedSchema.tid)
              && predictedErrorsWithTruthDF(FullResult.attrnr) === ddPrediction(DeepdivePredictedSchema.attr))


        /**
          * Computing the F-measure
          */

        val correctRepair: Long = joinedPredictions
          .where(predictedErrorsWithTruthDF("truth-value") === ddPrediction(predicted_value))
          .count()

        val performedRepair: Long = joinedPredictions
          .where(ddPrediction("expectation") > 0.5)
          .where(predictedErrorsWithTruthDF("value") =!= ddPrediction(predicted_value))
          .count()

        val totalErrors: Long = joinedPredictions
          .where(predictedErrorsWithTruthDF(FullResult.label) === "1.0")
          .count()

        val precision: Double = correctRepair / performedRepair.toDouble
        val recall: Double = correctRepair / totalErrors.toDouble
        val F_1: Double = 2.0 * precision * recall / (precision + recall)


        println(s"considered attributes: ${
          HospSchema
            .indexAttributes
            .filter(attrs => Seq(cityAttrNr, stateAttrNr).contains(attrs._2))
            .map(entry => entry._1)
            .mkString(",")
        }")
        println(s"Precision: $precision; Recall: $recall; F-1: $F_1")


        joinedPredictions
          .where(ddPrediction(DeepdivePredictedSchema.expectation) > 0.5)
          .where(predictedErrorsWithTruthDF("truth-value") === ddPrediction(predicted_value))
          .show(67)


      }

        def percentageFound(total: Long, foundByMethod: Long, msg: String): Unit = {
          val percent = NumbersUtil.round(((foundByMethod * 100) / total.toDouble), 4)
          println(s"$msg $percent %")
        }

    }


  }
}


object Playground extends App {
  HospSchema.indexAttributes.foreach(
    (entry => println(s"${entry._2} -> ${entry._1}"))
  )

}







