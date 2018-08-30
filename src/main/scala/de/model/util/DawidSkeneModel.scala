package de.model.util

import de.evaluation.data.schema.{DefaultSchema, Schema}
import de.experiments.ExperimentsCommonConfig
import de.util.ErrorNotation._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._

class DawidSkeneModel extends ExperimentsCommonConfig {

  private var schema: Schema = null
  private var datasetName: String = ""
  private var classifierColumns: Seq[String] = null

  private var matrix: DataFrame = null

  private val attrName: String = "attrName"
  private val label = "label"

  def onDataset(name: String): this.type = {
    datasetName = name
    schema = getSchema
    this
  }

  def onDataFrame(data: DataFrame): this.type = {
    matrix = data
    this
  }

  def onColumns(ecCols: Seq[String]): this.type = {
    classifierColumns = ecCols
    this
  }

  def createModel(): DataFrame = {
    val window: WindowSpec = Window.orderBy(schema.getRecID)
    val enumeratedItemsDF: DataFrame = matrix
      .select(schema.getRecID, attrName)
      .distinct()
      .withColumn("num", row_number().over(window))

    val fullDataWithNumsDF: DataFrame = matrix
      .join(enumeratedItemsDF, Seq(schema.getRecID, attrName))


    val ecColumns = classifierColumns.map(colName => col(colName))

    val modelDF: DataFrame = fullDataWithNumsDF.withColumn("ecToArray", array(ecColumns: _*))
      .select(col(label), col("num"), posexplode(col("ecToArray")).as(Seq("annotator-id", "y")))
      .drop("ecToArray")
      .where(col("y") =!= DOES_NOT_APPLY)
      .withColumn("y-tmp", when(col("y") === CLEAN, lit(0)).otherwise(col("y")))
      .drop("y")
      .withColumnRenamed("y-tmp", "y")
      .select("num", "annotator-id", "y", label)

    modelDF
  }


  private def getSchema: Schema = {
    allSchemasByName.getOrElse(datasetName, DefaultSchema)
  }

}

object DawidSkeneModel {
  def apply(): DawidSkeneModel = new DawidSkeneModel()
}
