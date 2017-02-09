package de.evaluation.data.gold.standard

import com.google.common.base.Strings
import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.{BlackOakSchema, Schema}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Fluent interface for ground truth creation and persistence
  */
object GoldStandardCreator {

  private var cleanData: String = ""
  //"data.BlackOak.clean-data-path"
  private var dirtyData: String = "" //"data.BlackOak.dirty-data-path"

  private var outputFolder: String = "" //"output.blackoak.goldstandard.ground.truth.folder"

  private var schema: Schema = null

  private var recid = "RecID"

  val conf = ConfigFactory.load()

  def onSchema(s: Schema): this.type = {
    schema = s
    recid = s.getRecID
    this
  }

  def addDirtyPath(dirty: String): this.type = {
    dirtyData = dirty
    this
  }

  def addCleanPath(clean: String): this.type = {
    cleanData = clean
    this
  }

  def specifyOutputFolder(folder: String): this.type = {
    outputFolder = folder
    this
  }

  def create: Unit = {
    SparkLOAN.withSparkSession("GROUNDTRUTH") {
      session => {
        val dirtyDF: DataFrame = DataSetCreator.createDataSet(session, dirtyData, schema.getSchema: _*)

        val cleanDF: DataFrame = DataSetCreator.createDataSet(session, cleanData, schema.getSchema: _*)

        val groundTruth: Dataset[String] = createLogGoldStandardWithGroundTruth(session, dirtyDF, cleanDF)
        groundTruth.show(12)
        val goldStdWithGroundTruth = conf.getString(outputFolder)
        groundTruth.coalesce(1).write.text(goldStdWithGroundTruth)
      }
    }
  }

  //todo: make this method general.
  private def createLogGoldStandardWithGroundTruth(sparkSession: SparkSession,
                                                   dirtyDF: DataFrame,
                                                   cleanDF: DataFrame): Dataset[String] = {
    // val schema = sch// BlackOakSchema.schema
    val indexedAttributes = BlackOakSchema.indexedAttributes

    val error = "1"
    val clean = "0"

    //here we produce log data for dirty rows and log dirty attributes:
    import sparkSession.implicits._
    val join = cleanDF
      .joinWith(dirtyDF, cleanDF.col(recid) === dirtyDF.col(recid))
      .flatMap(row => {
        //row._1.schema.fieldNames
        val cleanVals = row._1.getValuesMap[String](schema.getSchema)
        val dirtyVals = row._2.getValuesMap[String](schema.getSchema)

        val id = cleanVals.getOrElse(recid, "0")

        val dirtyAndCleanAttributes = for (attrName <- schema.getSchema; if attrName != recid) yield {
          val cleanValue = cleanVals.getOrElse(attrName, "")
          val dirtyValue = dirtyVals.getOrElse(attrName, "")

          val idxIfDistinct = if (!Strings.isNullOrEmpty(cleanValue) && !cleanValue.equals(dirtyValue)) {

            s"$id,${indexedAttributes.getOrElse(attrName, 0)},$error"
          } else {
            s"$id,${indexedAttributes.getOrElse(attrName, 0)},$clean"
          }
          idxIfDistinct.asInstanceOf[String]
        }
        dirtyAndCleanAttributes
      })


    join
  }


}
