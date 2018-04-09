package de.experiments.holoclean

import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.generation.FeaturesGenerator
import de.experiments.holoclean.HospHolocleanSchema._
import de.model.util.NumbersUtil
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

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




object HolocleanResultCoverter {
  def main(args: Array[String]): Unit = {
    val pathToData = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets"
    val holocleanPath = s"$pathToData/holoclean-output-ideal-error-detection.csv"
    SparkLOAN.withSparkSession("eval holoclean") {
      session => {


        val initOutputDF: DataFrame = DataSetCreator.createFrame(session, holocleanPath, Seq("ind", "attr", "val", "expectation"): _*)

        val convert_idx_to_name = udf {
          id: String => {
            val idx = String.valueOf(id).toInt
            HospHolocleanSchema.idxToAttr.getOrElse(idx, 0).toString
          }
        }

        val convertedOutput: DataFrame = initOutputDF
          .withColumn("attr-tmp", convert_idx_to_name(initOutputDF("attr")))
          .drop(col("attr"))
          .withColumnRenamed("attr-tmp", "attr")
          .select("ind", "attr", "val", "expectation")
        convertedOutput.show()

        convertedOutput
          .repartition(1)
          .write
          .option("header", "true")
          .csv(s"$pathToData/tmp")

      }


    }
  }
}

/*object HospHolocleanEvaluator {
  val groundtruthPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/groundtruth.csv"
  val dirtyDataPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/hospital_dataset.csv"

  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("eval holoclean") {
      session => {

        val dirtyDataDF: DataFrame = DataSetCreator.createFrame(session, dirtyDataPath, HospHolocleanSchema.schema: _*)
        val initvalueDF: DataFrame = dirtyDataDF
          .withColumn("valsToArray",
            array(index,
              ProviderNumber,
              HospitalName,
              Address1,
              Address2,
              Address3,
              City,
              State,
              ZipCode,
              CountyName,
              PhoneNumber,
              HospitalType,
              HospitalOwner,
              EmergencyService,
              Condition,
              MeasureCode,
              MeasureName,
              Score,
              Sample,
              Stateavg))
          .select(col(index), posexplode(col("valsToArray")).as(Seq("attr", "value")))
          .toDF(index, "attr", "dirty-value")

        val convert_to_idx = udf {
          attrName: String => {
            HospHolocleanSchema.attrToIdx.getOrElse(attrName, 0)
          }
        }
        val groundTruthPrepareDF: DataFrame = DataSetCreator.createFrame(session, groundtruthPath, Seq("ind", "attr", "val"): _*)

        val groundTruthDF: DataFrame = groundTruthPrepareDF
          .withColumn("attr-idx", convert_to_idx(groundTruthPrepareDF("attr")))
          .drop(col("attr"))
          .withColumnRenamed("attr-idx", "attr")
          .select("ind", "attr", "val")
          .toDF(index, "attr", "clean-value")


        /**
          * connecting to the database: deepdive results are in the table [query-predicate]_label_inference
          */
        val props: Properties = new Properties()
        props.put("user", "visenger")
        props.put("driver", "org.postgresql.Driver")

        val deep_dive_inference_result = "value_label_inference"
        val url = "jdbc:postgresql://localhost:5432/holoclean_hosp"

        //todo: precision recall f-1

        //todo:
      }
    }

  }
}*/

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

        val convert_to_double = udf {
          value: String => value.toDouble
        }

        val statPrecitionLabel: collection.Map[(String, String), Long] = samplePredictedDF.select(FullResult.label, "final-predictor").rdd.map(row => {
          val label = row.getString(0)
          val prediction = row.getString(1)
          (prediction, label)
        }).countByValue()

        println(statPrecitionLabel)

        var tp = 0.0
        var fn = 0.0
        var tn = 0.0
        var fp = 0.0

        statPrecitionLabel.foreach(elem => {
          val values = elem._1
          val count = elem._2
          values match {
            //confusion matrix
            //(prediction, label)
            case ("1.0", "1.0") => tp = count
            case ("1.0", "0.0") => fp = count
            case ("0.0", "1.0") => fn = count
            case ("0.0", "0.0") => tn = count
          }
        })

        //    println(s"true positives: $tp")

        val totalData = tp + fp + tn + fn

        val accuracy = (tp + tn) / totalData.toDouble
        //    println(s"Accuracy: $accuracy")
        val precision = tp / (tp + fp).toDouble
        println(s"Precision: $precision")

        val recall = tp / (tp + fn).toDouble
        println(s"Recall: $recall")

        val F1 = 2 * precision * recall / (precision + recall)
        println(s"F-1 Score: $F1")

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
  * prepares several datasets: groundtruth, dirty input
  */
object HospPreparator extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    val dataset = "hosp"
    val config = ConfigFactory.load()
    val cleanDataPath = config.getString(s"data.$dataset.clean.10k")
    val pathToData = "/Users/visenger/deepdive_notebooks/hosp-cleaning"
    val dirtyDataPath = s"$pathToData/dirty-data/hosp-dirty-1k.csv"
    val matrixWithPredictionPath = s"$pathToData/predicted-data/hosp-1k-predicted-errors.csv"
    val schema: Schema = allSchemasByName.getOrElse(dataset, HospSchema)

    val idxToName: Map[Int, String] = Map(0 -> "prno",
      1 -> "hospitalname",
      2 -> "address",
      3 -> "city",
      4 -> "state",
      5 -> "zip",
      6 -> "countryname",
      7 -> "phone",
      8 -> "hospitaltype",
      9 -> "hospitalowner",
      10 -> "emergencyservice",
      11 -> "condition",
      12 -> "mc",
      13 -> "measurename",
      14 -> "score",
      15 -> "sample",
      16 -> "stateavg")


    SparkLOAN.withSparkSession("Create Predicates") {
      session => {

        val convert_to_init_idx = udf {
          generated_idx: Int => {
            val attributeName: String = idxToName.getOrElse(generated_idx, 0).toString
            schema.indexLCAttributes.getOrElse(attributeName, "unknown").asInstanceOf[Int]
          }
        }
        //Tuple
        val dirtyDF: DataFrame = DataSetCreator.createFrame(session, dirtyDataPath, schema.getSchema: _*)

        val dirtyValuesDF: DataFrame = dirtyDF.withColumn("valsToArray",
          array("prno",
            "hospitalname",
            "address",
            "city",
            "state",
            "zip",
            "countryname",
            "phone",
            "hospitaltype",
            "hospitalowner",
            "emergencyservice",
            "condition",
            "mc",
            "measurename",
            "score",
            "sample",
            "stateavg"))
          .select(col(schema.getRecID), posexplode(col("valsToArray")).as(Seq("attr", "value")))
          .withColumn("attr-tmp", convert_to_init_idx(col("attr")))
          .drop("attr")
          .withColumnRenamed("attr-tmp", "attr")
          .select(schema.getRecID, "attr", "value")
          .toDF("ind", "attr", "val")

        dirtyValuesDF.show()

        dirtyValuesDF
          .repartition(1)
          .write
          .option("header", "true")
          .csv(s"$pathToData/tmp-dirty")

        val fullCleanDF: DataFrame = DataSetCreator.createFrame(session, cleanDataPath, schema.getSchema: _*)
        val cleanCols: Seq[Column] = schema.getSchema.map(attr => fullCleanDF(attr))
        val cleanDF: DataFrame = fullCleanDF
          .join(dirtyDF, Seq(schema.getRecID)).select(cleanCols: _*).toDF()

        val cleanValueDF: DataFrame = cleanDF
          .withColumn("valsToArray",
            array("prno",
              "hospitalname",
              "address",
              "city",
              "state",
              "zip",
              "countryname",
              "phone",
              "hospitaltype",
              "hospitalowner",
              "emergencyservice",
              "condition",
              "mc",
              "measurename",
              "score",
              "sample",
              "stateavg"))
          .select(col(schema.getRecID), posexplode(col("valsToArray")).as(Seq("attr", "value")))
          .withColumn("attr-tmp", convert_to_init_idx(col("attr")))
          .drop("attr")
          .withColumnRenamed("attr-tmp", "attr")
          .select(schema.getRecID, "attr", "value")
          .toDF("ind", "attr", "val")

        cleanValueDF
          .repartition(1)
          .write
          .option("header", "true")
          .csv(s"$pathToData/tmp-groundtruth")


        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)
      }
    }
  }
}

/**
  * Automatic generating the evidence (observed) predicates.
  */
object PredicatesCreator extends ExperimentsCommonConfig {
  val dataset = "hosp"
  val config = ConfigFactory.load()
  val cleanDataPath = config.getString(s"data.$dataset.clean.10k")
  val pathToData = "/Users/visenger/deepdive_notebooks/hosp-cleaning"
  val dirtyDataPath = s"$pathToData/dirty-data/hosp-dirty-1k.csv"
  val matrixWithPredictionPath = s"$pathToData/predicted-data/hosp-1k-predicted-errors.csv"
  val schema: Schema = allSchemasByName.getOrElse(dataset, HospSchema)

  val groundtruthPath = "/Users/visenger/deepdive_notebooks/hosp-cleaning/groundtruth/hosp-groundtruth.csv"

  val pathToExtDict = "/Users/visenger/research/datasets/zip-code/free-zipcode-database-Primary.csv"


  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("Create Predicates") {
      session => {
        //Tuple
        val dirtyDF: DataFrame = DataSetCreator.createFrame(session, dirtyDataPath, schema.getSchema: _*)

        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)

        val tupleDF: DataFrame = dirtyDF.select(schema.getRecID)

        /*tupleDF
          .repartition(1)
          .write
          .option("header", "false")
          .csv(s"$pathToData/input/tuple")*/

        //InitValue

        val initValueDF: DataFrame = predictedMatrixDF
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)

        /*initValueDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/initvalue")*/


        //Domain

        //todo: create perfect domain: we use the groundtruth to simulate the perfect error detection
        val groundtruthDF: DataFrame = DataSetCreator
          .createFrame(session, groundtruthPath, Seq("ind", "attr", "val"): _*)
          .withColumnRenamed("ind", FullResult.recid)
          .withColumnRenamed("attr", FullResult.attrnr)
          .withColumnRenamed("val", FullResult.value)
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)

        val cleanValuesDF: DataFrame = initValueDF.intersect(groundtruthDF).toDF()
        val dirtyValuesDF: DataFrame = initValueDF.except(groundtruthDF).toDF()

        val perfectDomainValues: DataFrame = cleanValuesDF
          .groupBy(col(FullResult.attrnr))
          .agg(collect_set(FullResult.value) as "domain")


        //        val domainDF: DataFrame = initValueDF
        //          //.join(domainByAttrDF, Seq(FullResult.attrnr), "full_outer") //joining with the full domain
        //          .join(perfectDomainValues, Seq(FullResult.attrnr), "full_outer")
        //          .select(col(FullResult.recid), col(FullResult.attrnr), explode(col("domain")).as("domain"))


        // todo done: populate evidence predicates with the domain over clean values.
        val predictedCleanValsDF: DataFrame = predictedMatrixDF
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)
          .where(col("final-predictor") === "0.0")
          .toDF()

        val cleanDomainByAttrDF: DataFrame = predictedCleanValsDF
          .groupBy(col(FullResult.attrnr))
          .agg(collect_set(FullResult.value) as "domain")


        /**
          * using org.apache.spark.sql.functions#explode(org.apache.spark.sql.Column)
          * to flatten the values list.
          */
        val domainDF: DataFrame = initValueDF
          .join(cleanDomainByAttrDF, Seq(FullResult.attrnr), "full_outer")
          .select(col(FullResult.recid), col(FullResult.attrnr), explode(col("domain")).as("domain"))


        /*domainDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/domain")*/

        //Value?

        /*  val prepareValueDF: DataFrame = dirtyValuesDF
            .join(perfectDomainValues, Seq(FullResult.attrnr), "full_outer")
            .select(col(FullResult.recid), col(FullResult.attrnr), explode(col("domain")).as("value"))
           val valueDF = prepareValueDF
            .union(cleanValuesDF)
            .withColumn("label", lit("\\N"))
            .where(col(FullResult.recid) =!= lit(""))
            .select(FullResult.recid, FullResult.attrnr, "value", "label")

          */

        //todo: value over predicted errors/clean vals -> current version has error detection f-1 50%
        val predictedDirtyValsDF: DataFrame = predictedMatrixDF
          .select(FullResult.recid, FullResult.attrnr, FullResult.value)
          .where(col("final-predictor") === "1.0")
          .toDF()
        val prepareValueDF: DataFrame = predictedDirtyValsDF
          .join(cleanDomainByAttrDF, Seq(FullResult.attrnr), "full_outer")
          .select(col(FullResult.recid), col(FullResult.attrnr), explode(col("domain")).as("value"))

        val valueDF: DataFrame = prepareValueDF
          .union(predictedCleanValsDF)
          .withColumn("label", lit("\\N"))
          .where(col(FullResult.recid) =!= lit(""))
          .select(FullResult.recid, FullResult.attrnr, "value", "label")

        //todo: Domain pruning
        val prunedDomainDF: DataFrame = pruneDomain(session, initValueDF, cleanDomainByAttrDF, predictedDirtyValsDF, dirtyDF, schema)

        /**
          * prunedDomainDF.printSchema()
          * root
          * |-- RecID: string (nullable = true)
          * |-- attrNr: string (nullable = true)
          * |-- value: string (nullable = false)
          * |-- prob-of-domain: double (nullable = false)
          *
          */

        (0.0 to 1.0).by(0.1).foreach(τ => {

          val prunedValueDF: DataFrame = prunedDomainDF
            .where(prunedDomainDF("prob-of-domain") >= τ)
            .select(FullResult.recid, FullResult.attrnr, FullResult.value)
            .union(predictedCleanValsDF)
            .withColumn("label", lit("\\N"))
            .where(col(FullResult.recid) =!= lit(""))
            .select(FullResult.recid, FullResult.attrnr, "value", "label")

          /*prunedValueDF
            .repartition(1)
            .write.option("sep", "\\t")
            .option("header", "false")
            .csv(s"$pathToData/input-${τ}/value-${τ}")*/
        })


        /* valueDF
           .repartition(1)
           .write.option("sep", "\\t")
           .option("header", "false")
           .csv(s"$pathToData/input/value")*/


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

        /* hasFeatureDF.repartition(1)
           .write
           .option("sep", "\\t")
           .option("header", "false")
           .csv(s"$pathToData/input/hasfeature")
 */

        //ExtDict
        val zipCodesDF: DataFrame = DataSetCreator.createFrame(session, pathToExtDict, ZipCodesDictSchema.schema: _*)

        val extDictDF: DataFrame = zipCodesDF.select("Zipcode", "City", "State")
          .withColumn("RowId", monotonically_increasing_id())
          .withColumn("DictNumber", lit(1))
          .withColumn("valsToArray", array("Zipcode", "City", "State"))
          .select(col("RowId"), posexplode(col("valsToArray")).as(Seq("extAttrNr  ", "value")), col("DictNumber"))

        /*extDictDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/extdict")*/

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

        /* matchedDF
           .repartition(1)
           .write
           .option("sep", "\\t")
           .option("header", "false")
           .csv(s"$pathToData/input/matched")*/
      }
    }
  }

  def pruneDomain(session: SparkSession,
                  initValueDF: DataFrame,
                  cleanDomainByAttrDF: DataFrame,
                  predictedDirtyValsDF: DataFrame,
                  dirtyDF: DataFrame,
                  schema: Schema): DataFrame = {

    import session.implicits._

    val otherTupleValues = "other-vals"
    /**
      * first aggregate by RecID, second for each tuple in aggregated bin,
      * concatenate its values as Map "attrNr->value"
      */
    val otherValsOfTuple: DataFrame = initValueDF.groupBy(col(FullResult.recid))
      .agg(collect_list(map(col(FullResult.attrnr), col(FullResult.value))) as otherTupleValues) //collects Map[String, String] as list
      .as[(String, Seq[Map[String, String]])]
      .map { case (id, list) => (id, list.reduce(_ ++ _)) } //joining all maps in the list
      .toDF(FullResult.recid, otherTupleValues)


    /**
      *
      *  otherValsOfTuple.printSchema()
      * root
      * |-- RecID: string (nullable = true)
      * |-- other-vals: map (nullable = true)
      * |    |-- key: string
      * |    |-- value: string (valueContainsNull = true)
      */

    val domainWithTupleValuesDF: DataFrame = predictedDirtyValsDF
      .join(cleanDomainByAttrDF, Seq(FullResult.attrnr), "full_outer")
      .join(otherValsOfTuple, Seq(FullResult.recid))


    /**
      *  domainWithTupleValuesDF.printSchema()
      * root
      * 0|-- RecID: string (nullable = true)
      * 1|-- attrNr: string (nullable = true)
      * 2|-- value: string (nullable = true)
      * 3|-- domain: array (nullable = true)
      * |    |-- element: string (containsNull = true)
      * 4|-- other-vals: map (nullable = true)
      * |    |-- key: string
      * |    |-- value: string (valueContainsNull = true)
      */


    val data: List[(String, String, String, Seq[String], collection.Map[String, String])] = domainWithTupleValuesDF
      .map(row => (row.getString(0), row.getString(1), row.getString(2), row.getSeq[String](3), row.getMap[String, String](4)))
      .collect()
      .toList

    val indxToAttrNames: Map[Int, String] = schema.indexLCAttributes.map(_.swap)

    val domainFull: List[(String, String, String, Map[String, Double])] = data
      //.take(10) //todo: remove take(x)
      .par
      .map(entry => {
        val recId: String = entry._1
        val attrNr: String = entry._2
        val value: String = entry._3
        val domain: Seq[String] = entry._4
        val tupleOtherVals: collection.Map[String, String] = entry._5

        val domainWithProbs: Seq[(String, Double)] = for {d <- domain
                                                          t <- tupleOtherVals} yield {
          val otherAttrNr: String = t._1
          val otherValue: String = t._2

          val domainAttrName: String = indxToAttrNames.getOrElse(attrNr.trim.toInt, "unknown")
          val otherAttrName: String = indxToAttrNames.getOrElse(otherAttrNr.trim.toInt, "unknown")

          val withOtherValDF: DataFrame = dirtyDF
            .select(domainAttrName, otherAttrName)
            .where(dirtyDF(otherAttrName) === otherValue).toDF()

          val togetherCount: Long = withOtherValDF.where(dirtyDF(domainAttrName) === d).count()

          val totalV_count: Long = withOtherValDF.count()

          val probOf_d_given_v: Double = togetherCount / totalV_count.toDouble

          (d, probOf_d_given_v)
        }

        val filteredDomain: Map[String, Double] = domainWithProbs.filter(dom => dom._2 > 0.0).toMap

        (recId, attrNr, value, filteredDomain)

      }).toList

    val tuplesWithDomain: List[(String, String, String, Map[String, Double])] = domainFull
      .filter(tuple => tuple._4 != null && tuple._4.size > 0)


    val domainWithProbsDF: DataFrame = tuplesWithDomain
      .toDF(FullResult.recid, FullResult.attrnr, FullResult.value, "domain-with-probs")
      .select(col(FullResult.recid), col(FullResult.attrnr), explode(col("domain-with-probs")).as(Seq("value", "prob-of-domain")))

    domainWithProbsDF


  }
}


//@deprecated
/*object EvaluateDeepdive extends ExperimentsCommonConfig {
  val dataset = "hosp"

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val cleanDataPath = config.getString(s"data.$dataset.clean.10k")
    val pathToData = "/Users/visenger/deepdive_notebooks/hosp-cleaning"
    val matrixWithPredictionPath = s"$pathToData/predicted-data/hosp-1k-predicted-errors.csv"
    val dirtyInput = s"$pathToData/dirty-data/hosp-dirty-1k.csv"

    SparkLOAN.withSparkSession("EVAL DeepDive") {
      session => {

        val predictedMatrixDF: DataFrame = DataSetCreator
          .createFrame(session, matrixWithPredictionPath, HospPredictedSchema.schema: _*)


        val datasetSchema = allSchemasByName.getOrElse(dataset, HospSchema)
        val schema: Seq[String] = datasetSchema.getSchema
        val cleanData: DataFrame = DataSetCreator.createFrame(session, cleanDataPath, schema: _*)
        val truthValue = "truth-value"

        cleanData.printSchema()

        cleanData.show()



        //        val predictedErrorsWithTruthDF: DataFrame = predictedMatrixDF
        //          .join(cleanDF, Seq(FullResult.recid, FullResult.attrnr))
        //          .select(FullResult.recid, FullResult.attrnr, FullResult.value, FullResult.label, "final-predictor", "truth-value")

        /**
          * connecting to the database: deepdive results are in the table [query-predicate]_label_inference
          */
        val props: Properties = new Properties()
        props.put("user", "visenger")
        props.put("driver", "org.postgresql.Driver")

        val deep_dive_inference_result = "value_label_inference"
        val url = "jdbc:postgresql://localhost:5432/deepdive_hosp"

        val ddValuePredictionTable: DataFrame = session
          .read
          .jdbc(url, deep_dive_inference_result, props)
          .toDF(DeepdivePredictedSchema.schema: _*)

        //todo evaluation


        def percentageFound(total: Long, foundByMethod: Long, msg: String): Unit = {
          val percent = NumbersUtil.round(((foundByMethod * 100) / total.toDouble), 4)
          println(s"$msg $percent %")
        }

      }


    }
  }
}*/




object Playground extends App {
  HospSchema.indexAttributes.foreach(
    (entry => println(s"${entry._2} -> ${entry._1}"))
  )

}







