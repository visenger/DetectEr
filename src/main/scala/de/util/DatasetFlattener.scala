package de.util

import de.evaluation.data.schema.{DefaultSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.DataSetCreator
import de.experiments.ExperimentsCommonConfig
import org.apache.spark.sql.functions.{array, col, posexplode, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DatasetFlattener extends ExperimentsCommonConfig {

  private var schema: Schema = null
  private var datasetName: String = ""
  private var cleanDataPath = ""
  private var dirtyDataPath = ""

  private val attrName: String = "attrName"
  private val label = "label"
  private val value = "value"

  def onDataset(name: String): this.type = {
    datasetName = name
    schema = getSchema
    cleanDataPath = getCleanPath
    dirtyDataPath = getDirtyPath
    this
  }

  def flattenDataFrame(dirtyDF: DataFrame): DataFrame = {
    val flattenedDF: DataFrame = internalFlattener(dirtyDF)
    flattenedDF
  }

  def flattenDirtyData(session: SparkSession): DataFrame = {
    val flattenedDF: DataFrame = flattenData(session, dirtyDataPath)
    flattenedDF
  }

  def flattenCleanData(session: SparkSession): DataFrame = {
    val flattenedDF: DataFrame = flattenData(session, cleanDataPath)
    flattenedDF
  }

  def makeFlattenedDiff(session: SparkSession): DataFrame = {

    val flatCleanDF: DataFrame = flattenCleanData(session)
    val flatDirtyDF: DataFrame = flattenDirtyData(session)

    val diffDF: DataFrame = makeInternalDiff(flatCleanDF, flatDirtyDF)
    diffDF
  }

  def makeFlattenedDiff(flatCleanDF: DataFrame, flatDirtyDF: DataFrame): DataFrame = {

    val diffDF: DataFrame = makeInternalDiff(flatCleanDF, flatDirtyDF)
    diffDF
  }


  private def makeInternalDiff(flatCleanDF: DataFrame, flatDirtyDF: DataFrame): DataFrame = {
    val create_label = udf {
      (are_vals_equal: Boolean) => {
        are_vals_equal match {
          case true => 0
          case false => 1
        }
      }
    }

    val diffDF: DataFrame = flatCleanDF
      .join(flatDirtyDF, Seq(schema.getRecID, "attrName"))
      .withColumn("diff", flatCleanDF(value) <=> flatDirtyDF(value))
      .withColumn(label, create_label(col("diff")))
      .select(col(schema.getRecID), col(attrName), flatDirtyDF(value).as("dirty-value"), flatCleanDF(value).as("clean-value"), col(label))
    diffDF
  }

  private def flattenData(session: SparkSession, dataPath: String): DataFrame = {
    import org.apache.spark.sql._

    val dirtyDF: DataFrame = DataSetCreator.createFrame(session, dataPath, schema.getSchema: _*)

    val flattenedDF: DataFrame = internalFlattener(dirtyDF)
    flattenedDF
  }


  private def internalFlattener(dirtyDF: DataFrame): DataFrame = {
    val recID: String = schema.getRecID

    val nonIdAttrs: Seq[String] = schema.getSchema.diff(Seq(recID))

    val idxToAttribute: Map[Int, String] = schema
      .getSchema
      .zipWithIndex
      .map(_.swap)
      .map { case t: Tuple2[Int, String] => (t._1, t._2) }
      .toMap[Int, String]


    val convert_id_to_attrName = udf {
      idx: Int => idxToAttribute.getOrElse(idx, 0).asInstanceOf[String]
    }

    val value = FullResult.value
    val valsToArray = "valsToArray"

    val flattenedDF: DataFrame = dirtyDF.withColumn(valsToArray, array(recID, nonIdAttrs: _*))
      .select(col(recID), posexplode(col(valsToArray)).as(Seq("pos", value)))
      .withColumn(attrName, convert_id_to_attrName(col("pos")))
      .drop(valsToArray)
      .select(recID, attrName, "pos", value)
    flattenedDF
  }

  def getDirtyPath: String = {
    allRawData.getOrElse(datasetName, "incorrect dataset name for dirty path")
  }

  def getCleanPath: String = {
    allCleanData.getOrElse(datasetName, "incorrect dataset name for clean path")
  }

  def getSchema: Schema = {
    allSchemasByName.getOrElse(datasetName, DefaultSchema)
  }
}

object DatasetFlattener {
  def apply(): DatasetFlattener = new DatasetFlattener()
}