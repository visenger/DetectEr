package de.evaluation.tools.pattern.violation

import com.typesafe.config.ConfigFactory
import de.evaluation.data.blackoak.BlackOakSchema
import de.evaluation.f1.DataF1
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
  * Created by visenger on 25/12/16.
  */
class TrifactaResults {
  val conf = ConfigFactory.load()
  private val cleanData = "data.BlackOak.clean-data-path"
  private val dirtyData = "data.BlackOak.dirty-data-path"
  private val trifactaData = "trifacta.cleaning.result"

  def convertPatternViolationResult(sparkSession: SparkSession): DataFrame = {
    import scala.collection.JavaConversions._
    import sparkSession.implicits._


    val attributesNames = conf.getStringList("trifacta.fields").toList
    /** read attribute names from config file   */

    /*what was cleaned*/
    val trifactaDF: DataFrame = DataSetCreator
      .createDataSet(sparkSession, trifactaData, BlackOakSchema.schema: _*)
    val trifactaCols: List[Column] = getColumns(trifactaDF, attributesNames)
    val trifactaProjection: DataFrame = trifactaDF.select(trifactaCols: _*)

    /*what was dirty*/
    val dirtyBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, dirtyData, BlackOakSchema.schema: _*)
    val dirtyDFCols: List[Column] = getColumns(dirtyBlackOakDF, attributesNames)
    val dirtyDataProjection = dirtyBlackOakDF.select(dirtyDFCols: _*)


    val whatTrifactaFound = dirtyDataProjection.except(trifactaProjection)

    val recid = "RecID"
    val nonIdAttributes = attributesNames.diff(Seq(recid))
    val idxOfAttributes: List[Int] = nonIdAttributes
      .map(attr => BlackOakSchema.indexedLowerCaseAttributes.getOrElse(attr.toLowerCase, 0))

    val extendedPatternVioFields: Dataset[(String, String)] = whatTrifactaFound.toDF().flatMap(row => {
      val id = row.getAs[String](recid)
      val foundFields: List[(String, String)] = idxOfAttributes.map(attrIdx => (id, attrIdx.toString))
      foundFields
    })


    /** todo: write extended duplicates into a file */

    extendedPatternVioFields.toDF(DataF1.schema: _*)
  }

  def getPatternViolationResult(session: SparkSession): DataFrame = {
    val confString = "output.trifacta.result.file"
    val trifactaOutput = DataSetCreator.createDataSetNoHeader(session, confString, DataF1.schema: _*)
    trifactaOutput
  }

  def writeResultToDisk(): Unit = {
    SparkLOAN.withSparkSession("tfct") {
      session => {
        val foundPatternVio: DataFrame = convertPatternViolationResult(session)
        foundPatternVio.write.csv(conf.getString("output.trifacta.result.folder"))
      }
    }
  }

  private def getColumns(df: DataFrame, attributesNames: List[String]) = {
    attributesNames.map(attr => df.col(attr))
  }


}

object TrifactaResults {

  def main(args: Array[String]): Unit = {
    //new TrifactaResults().writeResultToDisk()
  }

  def getPatternViolationResult(session: SparkSession): DataFrame = {
    new TrifactaResults().getPatternViolationResult(session)
  }

  def convertPatternViolationResult(sparkSession: SparkSession): DataFrame = {
    new TrifactaResults().convertPatternViolationResult(sparkSession)
  }

}
