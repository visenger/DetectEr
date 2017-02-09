package de.evaluation.data.blackoak

import com.google.common.base.Strings
import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.gold.standard.GoldStandardCreator
import de.evaluation.data.schema.{BlackOakSchema, Schema}
import de.evaluation.f1.{DataF1, Table}
import de.evaluation.util.{DataSetCreator, SparkLOAN, SparkSessionCreator}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


class BlackOakGoldStandard {

  val conf: Config = ConfigFactory.load()

  val cleanData = "data.BlackOak.clean-data-path"
  val dirtyData = "data.BlackOak.dirty-data-path"

  val goldStdFile: String = "output.blackouak.gold.file"

  val outputFolder = "output.blackoak.goldstandard.ground.truth.folder"

  val groundTruthFile = "output.blackoak.goldstandard.ground.truth.file"


  @Deprecated
  def createGoldStandard(): Unit = {

    val sparkSession: SparkSession = SparkSessionCreator.createSession("GOLD")

    val dirtyBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, dirtyData, BlackOakSchema.getSchema: _*)

    val cleanBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, cleanData, BlackOakSchema.schema: _*)

    val goldStandard: Dataset[String] = createLogGoldStandard(sparkSession, dirtyBlackOakDF, cleanBlackOakDF)


    val outputGoldStandard = conf.getString("output.blackouak.goldstandard")
    goldStandard.write.text(outputGoldStandard)


    // always close your resources
    sparkSession.stop()
  }

  def createGoldWithGroundTruth(): Unit = {
    val creator = GoldStandardCreator
    val s = BlackOakSchema
    creator.onSchema(s)
    creator.addDirtyPath(dirtyData)
    creator.addCleanPath(cleanData)
    creator.specifyOutputFolder("output.blackoak.goldstandard.ground.truth.folder")
    creator.create
  }


  def old_createGoldWithGroundTruth(): Unit = {
    SparkLOAN.withSparkSession("GROUNDTRUTH") {
      session => {
        val dirtyBlackOakDF: DataFrame = DataSetCreator.createDataSet(session, dirtyData, BlackOakSchema.schema: _*)

        val cleanBlackOakDF: DataFrame = DataSetCreator.createDataSet(session, cleanData, BlackOakSchema.schema: _*)

        val groundTruth: Dataset[String] = createLogGoldStandardWithGroundTruth(session, dirtyBlackOakDF, cleanBlackOakDF)

        val goldStdWithGroundTruth = conf.getString("output.blackoak.goldstandard.ground.truth.folder")
        groundTruth.coalesce(1).write.text(goldStdWithGroundTruth)
      }
    }
  }

  def getGoldStandard(session: SparkSession): DataFrame = {

    val goldStd: DataFrame = DataSetCreator.createDataSetNoHeader(session, goldStdFile, DataF1.schema: _*)
    goldStd
  }

  def getGroundTruth(session: SparkSession): DataFrame = {
    val groundTruth: DataFrame = DataSetCreator.createDataSetNoHeader(session, groundTruthFile, Table.schema: _*)
    groundTruth
  }

  private val recid = "RecID"

  @Deprecated
  private def createLogGoldStandard(sparkSession: SparkSession, dirtyBlackOakDF: DataFrame, cleanBlackOakDF: DataFrame): Dataset[String] = {
    val schema = BlackOakSchema.schema
    val indexedAttributes = BlackOakSchema.indexedAttributes

    //here we produce log data for dirty rows and log dirty attributes:
    import sparkSession.implicits._
    val join = cleanBlackOakDF
      .joinWith(dirtyBlackOakDF, cleanBlackOakDF.col(recid) === dirtyBlackOakDF.col(recid))
      .flatMap(row => {
        val cleanVals = row._1.getValuesMap[String](schema)
        val dirtyVals = row._2.getValuesMap[String](schema)

        val dirtyAttributes = for (attrName <- schema; if attrName != recid) yield {
          val cleanValue = cleanVals.getOrElse(attrName, "")
          val dirtyValue = dirtyVals.getOrElse(attrName, "")

          val idxIfDistinct = if (!Strings.isNullOrEmpty(cleanValue) && !cleanValue.equals(dirtyValue)) {
            s"${indexedAttributes.getOrElse(attrName, 0)}"
          } else ""
          idxIfDistinct.asInstanceOf[String]
        }
        val id = cleanVals.getOrElse(recid, "0")
        val nonEmptyAttrs = dirtyAttributes.filter(_.nonEmpty)


        /*flatten*/
        val flattenedRow: Seq[String] = nonEmptyAttrs.map(a => s"$id,$a") /**/

        flattenedRow
      })


    join
  }


  //todo: make this method general.
  private def createLogGoldStandardWithGroundTruth(sparkSession: SparkSession,
                                                   dirtyBlackOakDF: DataFrame,
                                                   cleanBlackOakDF: DataFrame): Dataset[String] = {
    val schema = BlackOakSchema.schema
    val indexedAttributes = BlackOakSchema.indexedAttributes

    val error = "1"
    val clean = "0"

    //here we produce log data for dirty rows and log dirty attributes:
    import sparkSession.implicits._
    val join = cleanBlackOakDF
      .joinWith(dirtyBlackOakDF, cleanBlackOakDF.col(recid) === dirtyBlackOakDF.col(recid))
      .flatMap(row => {
        row._1.schema.fieldNames
        val cleanVals = row._1.getValuesMap[String](schema)
        val dirtyVals = row._2.getValuesMap[String](schema)

        val id = cleanVals.getOrElse(recid, "0")

        val dirtyAndCleanAttributes = for (attrName <- schema; if attrName != recid) yield {
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

object BlackOakGoldStandardRunner {
  def main(args: Array[String]): Unit = {
    // new BlackOakGoldStandard().createGoldStandard()
    new BlackOakGoldStandard().createGoldWithGroundTruth()
  }

  def getGroundTruth(session: SparkSession): DataFrame = {
    new BlackOakGoldStandard().getGroundTruth(session)
  }

  @Deprecated
  def getGoldStandard(session: SparkSession): DataFrame = {
    new BlackOakGoldStandard().getGoldStandard(session)
  }
}


object Playground extends App {

  val schema = Seq("RecID", "FirstName", "MiddleName", "LastName", "Address", "City", "State", "ZIP", "POBox", "POCityStateZip", "SSN", "DOB")

  val indexedAttributes: Map[String, Int] = schema.zipWithIndex.toMap.map(
    e => (e._1, {
      var i = e._2;
      i += 1; // 1-based indexing
      i
    }))
  indexedAttributes.foreach(println)

}
