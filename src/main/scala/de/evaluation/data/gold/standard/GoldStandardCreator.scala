package de.evaluation.data.gold.standard

import com.google.common.base.Strings
import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.Schema
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


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

  //TODO: asap solve the join problem get back to the general solution
  def dirtySolution(numRows: Int): Unit = {
    SparkLOAN.withSparkSession("GROUNDTRUTH") {
      session => {
        val dirtyDF: DataFrame = DataSetCreator.createDataSet(session, dirtyData, schema.getSchema: _*)
        println(s"dirty:")
        dirtyDF.explain()

        val cleanDF: DataFrame = DataSetCreator.createDataSet(session, cleanData, schema.getSchema: _*)
        println(s"clean:")
        cleanDF.explain()

        val groundTruth: Dataset[String] = hospGoldStandardWithGroundTruth(session, dirtyDF, cleanDF)
        println(s"ground truth:")
        //groundTruth.show(numRows)
        val goldStdWithGroundTruth = conf.getString(outputFolder)

        groundTruth.write.text(goldStdWithGroundTruth)
      }
    }
  }


  def show(numRows: Int): Unit = {
    SparkLOAN.withSparkSession("GROUNDTRUTH") {
      session => {
        val dirtyDF: DataFrame = DataSetCreator.createDataSet(session, dirtyData, schema.getSchema: _*)
        println(s"dirty:")
        dirtyDF.show(numRows)
        dirtyDF.explain()

        val cleanDF: DataFrame = DataSetCreator.createDataSet(session, cleanData, schema.getSchema: _*)
        println(s"clean:")
        cleanDF.show(numRows)
        cleanDF.explain()

        val groundTruth: Dataset[String] = createLogGoldStandardWithGroundTruth(session, dirtyDF, cleanDF)
        println(s"ground truth:")
        groundTruth.show(numRows)
        //        val goldStdWithGroundTruth = conf.getString(outputFolder)
        //        groundTruth.coalesce(1).write.text(goldStdWithGroundTruth)
      }
    }
  }

  def create: Unit = {
    SparkLOAN.withSparkSession("GROUNDTRUTH") {
      session => {
        val dirtyDF: DataFrame = DataSetCreator.createDataSet(session, dirtyData, schema.getSchema: _*)

        val cleanDF: DataFrame = DataSetCreator.createDataSet(session, cleanData, schema.getSchema: _*)

        val groundTruth: Dataset[String] = createLogGoldStandardWithGroundTruth(session, dirtyDF, cleanDF)
        val goldStdWithGroundTruth = conf.getString(outputFolder)
        groundTruth.coalesce(1).write.text(goldStdWithGroundTruth)
      }
    }
  }

  //TODO: Achtung! this is quick&dirty solution -> because join ist not working with on hosp data.... still figuring out why :|
  private def hospGoldStandardWithGroundTruth(sparkSession: SparkSession,
                                              dirtyDF: DataFrame,
                                              cleanDF: DataFrame): Dataset[String] = {

    val indexedAttributes = schema.indexAttributes

    val error = "1"
    val clean = "0"

    //here we produce log data for dirty rows and log dirty attributes:
    import sparkSession.sqlContext.implicits._

    val cleanRdd: RDD[Row] = cleanDF.rdd
    val dirtyRdd: RDD[Row] = dirtyDF.rdd

    val joiningWithRDDs: Array[Seq[String]] = for (
      cleanRow: Row <- cleanRdd.take(cleanRdd.count().toInt);
      if !dirtyRdd.
        filter(_.getAs[String](recid).equals(cleanRow.getString(0))).isEmpty()
    ) yield {

      val dirtyRow = dirtyRdd.
        filter(_.getAs[String](recid).equals(cleanRow.getString(0))).first()

      val cleanVals = cleanRow.getValuesMap[String](schema.getSchema)
      val dirtyVals = dirtyRow.getValuesMap[String](schema.getSchema)
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
    }

    sparkSession.createDataset(joiningWithRDDs.flatten)

  }


  //todo: make this method general.
  private def createLogGoldStandardWithGroundTruth(sparkSession: SparkSession,
                                                   dirtyDF: DataFrame,
                                                   cleanDF: DataFrame): Dataset[String] = {
    // val schema = sch// BlackOakSchema.schema
    val indexedAttributes = schema.indexAttributes //BlackOakSchema.indexedAttributes

    val error = "1"
    val clean = "0"

    //here we produce log data for dirty rows and log dirty attributes:
    import sparkSession.sqlContext.implicits._


    val joinWith = cleanDF
      .joinWith(dirtyDF, cleanDF.col(recid) === dirtyDF.col(recid))

    joinWith.show()
    val join = joinWith
      .flatMap(row => {
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

  //todo: duplication! this method extends the clean dataframe with an row_id and uses this row_id as recId for the ground truth.
  private def external_createLogGoldStandardWithGroundTruth(sparkSession: SparkSession,
                                                            dirtyDF: DataFrame,
                                                            cleanDF: DataFrame): Dataset[String] = {
    // val schema = sch// BlackOakSchema.schema
    import org.apache.spark.sql.functions._
    import sparkSession.sqlContext.implicits._


    val indexedAttributes = schema.indexAttributes //BlackOakSchema.indexedAttributes

    val error = "1"
    val clean = "0"

    val rowid = "rowid"
    //here we produce log data for dirty rows and log dirty attributes:
    val cleanDFWithRowID = cleanDF.withColumn(rowid, lit(monotonically_increasing_id() + 1))

    val joinWith = cleanDFWithRowID
      .joinWith(dirtyDF, cleanDFWithRowID.col(recid) === dirtyDF.col(recid))

    val join = joinWith
      .flatMap(row => {
        val cleanVals = row._1.getValuesMap[String](schema.getSchema ++ Seq(rowid))
        val dirtyVals = row._2.getValuesMap[String](schema.getSchema)
        val id = cleanVals.getOrElse(rowid, 0)

        val dirtyAndCleanAttributes = for (attrName <- schema.getSchema;
                                           if (attrName != recid && attrName != rowid))
          yield {
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


  def external_GoldStandard() = {
    val config = ConfigFactory.load("external.conf")
    SparkLOAN.withSparkSession("EXTERNAL-GROUND-TRUTH") {
      session => {

        val cleanDF = DataSetCreator.createFrame(session, config.getString(cleanData), schema.getSchema: _*)

        val dirtyDF = DataSetCreator.createFrame(session, config.getString(dirtyData), schema.getSchema: _*)

        val externalGoldStandard: Dataset[String] = external_createLogGoldStandardWithGroundTruth(session, dirtyDF, cleanDF)
        externalGoldStandard.show()

        val goldStdWithGroundTruth = config.getString(outputFolder)
        externalGoldStandard.coalesce(1).write.text(goldStdWithGroundTruth)
      }
    }
  }


}
