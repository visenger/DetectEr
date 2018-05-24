package de.playground

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.{FlightsSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.SparkLOAN
import de.experiments.ExperimentsCommonConfig
import de.util.DatasetFlattener
import de.util.ErrorNotation._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable

object UDF {

  val identify_missing = udf {
    isNull: Boolean =>
      isNull match {
        case true => ERROR
        case false => CLEAN
      }
  }

}

trait ConfigBase {

  val commonsConfig: Config = ConfigFactory.load("data-commons.conf")
  val VALID_NUMBER = commonsConfig.getString("valid.number")

  val strDefaultValues: Seq[String] = commonsConfig.getStringList("default.string").asScala
  val intDefaultValues: Seq[String] = commonsConfig.getStringList("default.integer").asScala
  val dateDefaultValues: Seq[String] = commonsConfig.getStringList("default.date").asScala


}

/**
  * prototype to an error detection based on metadata information;
  */
object MetadataBasedErrorDetectorPlaygroud extends ExperimentsCommonConfig with ConfigBase {

  def getDefaultsByDataType(dataType: String): Seq[String] = {
    dataType match {
      case "String" => strDefaultValues
      case "Integer" => intDefaultValues
      case "Date/Time" => dateDefaultValues
      //todo: extend with more defaults
      case _ => intDefaultValues
    }
  }

  def getContainsInDefaults(defaultValues: Seq[String], value: String): Int = {
    val containsInDefaults: Boolean = defaultValues.map(_.toLowerCase()).contains(value.toLowerCase())

    containsInDefaults match {
      case true => ERROR
      case false => CLEAN
    }
  }

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("metadata reader") {
      session => {

        Seq("flights", "beers").foreach(dataset => {
          println(s"processing $dataset.....")

          val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val fullMetadataDF: DataFrame = creator.getFullMetadata(session, metadataPath)
          fullMetadataDF.show(false)
          //          fullMetadataDF.printSchema()


          val flatWithLabelDF: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)

          val flatWithMetadataDF: DataFrame = flatWithLabelDF.join(fullMetadataDF, Seq("attrName"))

          //Error Classifier #1
          //          val identify_missing = udf {
          //            isNull: Boolean =>
          //              isNull match {
          //                case true => ERROR
          //                case false => CLEAN
          //              }
          //          }

          //Error Classifier #2
          val schema: Schema = allSchemasByName.getOrElse(dataset, FlightsSchema)
          val dataTypesDictionary: Map[String, String] = schema.dataTypesDictionary

          val identify_default_values = udf {
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
                // remove default values from the top10
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



          //Error Classifier #5: cardinality estimation for PK columns
          //todo: specify unique columns -> PK columns for the dataset (e.g config entry)
          println(s"loading configurations for the dataset $dataset")

          val datasetConfig: Config = ConfigFactory.load(s"$dataset.conf")

          val uniqueColumns: Seq[String] = datasetConfig.getStringList("column.unique").asScala.toSeq

          val listOfAttsViolatesCardinality: List[String] = fullMetadataDF
            .select("attrName")
            .where(fullMetadataDF("attrName").isin(uniqueColumns: _*) && fullMetadataDF("% of distinct vals") =!= 100)
            .collect().map(r => r.getString(0)).toList

          println(s" this attr violates cardinality: ${listOfAttsViolatesCardinality.mkString(",")}")
          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)

          //todo: we identify all tuples of unique attributes which violates cadinality
          val allTuplesViolatingCardinality: List[String] = listOfAttsViolatesCardinality.flatMap(attr => {

            dirtyDF.where(dirtyDF("attrName") === attr)
              .groupBy(dirtyDF(FullResult.value))
              .agg(collect_set(dirtyDF(schema.getRecID)).as("tid-set"), count(dirtyDF(schema.getRecID)).as("count"))
              .where(col("count") > 1)
              .select(col("tid-set"))
              .rdd
              .flatMap(row => row.getSeq[String](0))
              .collect()

          }).toSet.toList


          println(s"the number of tuples violating cardinality constraint: ${allTuplesViolatingCardinality.size}")


          flatWithMetadataDF
            .withColumn("ec-missing-value", UDF.identify_missing(flatWithMetadataDF("dirty-value").isNull))
            .withColumn("ec-default-value",
              identify_default_values(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .withColumn("ec-top-value", is_top_value(flatWithMetadataDF("dirty-value"), flatWithMetadataDF("attrName"), flatWithMetadataDF("top10")))
            .withColumn("ec-valid-number", is_valid_number(flatWithMetadataDF("attrName"), flatWithMetadataDF("dirty-value")))
            .withColumn("ec-cardinality-vio",
              when(flatWithMetadataDF(schema.getRecID).isin(allTuplesViolatingCardinality: _*), lit(ERROR))
                .otherwise(lit(DOES_NOT_APPLY)))
            .where(flatWithMetadataDF("label") === 1).show(false)


        })


      }
    }
  }
}


