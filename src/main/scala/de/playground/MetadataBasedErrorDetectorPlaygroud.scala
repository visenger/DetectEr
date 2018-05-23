package de.playground

import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.{FlightsSchema, Schema}
import de.evaluation.util.SparkLOAN
import de.experiments.ExperimentsCommonConfig
import de.util.DatasetFlattener
import de.util.ErrorNotation._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable

/**
  * prototype to an error detection based on metadata information;
  */
object MetadataBasedErrorDetectorPlaygroud extends ExperimentsCommonConfig {

  val VALID_NUMBER = "^(0|[1-9]\\d*)?(\\.\\d+)?(?<=\\d)$"

  //todo: create default-values.config file and load defaults from there.
  val strDefaultValues: Seq[String] = Seq("Not Available", "-", "N/A", "unknown", "null", "Not provided by airline")
  val intDefaultValues: Seq[String] = Seq("Not Available", "-", "N/A", "unknown", "null", "NaN")
  val dateDefaultValues: Seq[String] = Seq("Not Available", "-", "N/A", "unknown", "null")


  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("metadata reader") {
      session => {

        Seq("flights", "beers").foreach(dataset => {
          println(s"processing $dataset.....")
          val metadataPath = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val fullMetadataDF: DataFrame = creator.getFullMetadata(session, metadataPath)
          //          fullMetadataDF.printSchema()

          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)

          val flatWithLabelDF: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)

          val flatWithMetadataDF: DataFrame = flatWithLabelDF.join(fullMetadataDF, Seq("attrName"))

          //Error Classifier #1
          val identify_missing = udf {
            isNull: Boolean =>
              isNull match {
                case true => ERROR
                case false => CLEAN
              }
          }

          //Error Classifier #2
          val schema: Schema = allSchemasByName.getOrElse(dataset, FlightsSchema)
          val dataTypesDictionary: Map[String, String] = schema.dataTypesDictionary


          def getContainsInDefaults(defaultValues: Seq[String], value: String): Int = {
            val containsInDefaults: Boolean = defaultValues.map(_.toLowerCase()).contains(value.toLowerCase())

            containsInDefaults match {
              case true => ERROR
              case false => CLEAN
            }
          }

          def getDefaultsByDataType(dataType: String): Seq[String] = {
            dataType match {
              case "String" => strDefaultValues
              case "Integer" => intDefaultValues
              case "Date/Time" => dateDefaultValues
              //todo: extend with more defaults
              case _ => intDefaultValues
            }
          }

          val identify_str_default = udf {
            (attrName: String, value: String) => {

              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "")

                dataType match {
                  case "String" => result = getContainsInDefaults(strDefaultValues, value)
                  case "Integer" => result = getContainsInDefaults(intDefaultValues, value)
                  case "Date/Time" => result = getContainsInDefaults(dateDefaultValues, value)
                  case _ => result = DOES_NOT_APPLY
                }
              }
              result
            }
          }

          //Error classifier #3: top-values

          val is_top_value = udf {
            (value: String, attrName: String, top10Values: mutable.Seq[String]) => {

              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                //todo: remove default values from the top10
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "")
                val defaults: Seq[String] = getDefaultsByDataType(dataType).map(_.toLowerCase())

                val topValsWithoutDefaults: mutable.Seq[String] = top10Values.map(_.toLowerCase()).diff(defaults)

                val valueInTop10: Boolean = topValsWithoutDefaults.contains(value.toLowerCase())
                valueInTop10 match {
                  case true => result = CLEAN
                  case false => result = ERROR
                }
              }
              result
            }
          }

          //Error Classifier #4: Semantic role for numbers

          def isValidNumber(value: String): Int = {

            val allDigits: Boolean = value.matches(VALID_NUMBER)
            allDigits match {
              case true => CLEAN
              case false => ERROR
            }
          }

          val is_valid_number = udf {
            (attrName: String, value: String) => {
              var result: Int = DOES_NOT_APPLY

              if (value != null) {
                //todo: remove default values from the top10
                val dataType: String = dataTypesDictionary.getOrElse(attrName, "")

                dataType match {
                  case "String" => result = DOES_NOT_APPLY
                  case "Integer" => result = isValidNumber(value)
                  case "Date/Time" => result = DOES_NOT_APPLY
                  case _ => result = DOES_NOT_APPLY
                }
              }
              result
            }
          }

          flatWithMetadataDF
            .withColumn("ec-missing-value", identify_missing(flatWithMetadataDF("dirty-value").isNull))
            .withColumn("ec-default-value",
              identify_str_default(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .withColumn("ec-top-value", is_top_value(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("attrName"), flatWithMetadataDF("top10")))
            .withColumn("ec-valid-number", is_valid_number(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .where(flatWithMetadataDF("label") === 1).show(false)


        })


      }
    }
  }
}
