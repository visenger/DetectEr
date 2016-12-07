package de.evaluation.data.blackoak

import com.google.common.base.Strings
import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.util.{DataSetCreator, SparkSessionCreator}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by visenger on 27/11/16.
  */

object BlackOakSchema {
  val schema = Seq("RecID", "FirstName", "MiddleName", "LastName", "Address", "City", "State", "ZIP", "POBox", "POCityStateZip", "SSN", "DOB")

  val indexedAttributes: Map[String, Int] = indexAttributes

  val indexedLowerCaseAttributes: Map[String, Int] = indexLCAttributes

  private def indexAttributes = {
    schema.zipWithIndex.toMap.map(
      e => (e._1, {
        var i = e._2;
        i += 1; // 1-based indexing
        i
      }))
  }

  private def indexLCAttributes: Map[String, Int] = {
    indexedAttributes.map(a => (a._1.toLowerCase, a._2))
  }

  def getIndexesByAttrNames(attributes: List[String]): List[Int] = {
    val allAttrToLowerCase = attributes.map(_.toLowerCase)
    val indexes = allAttrToLowerCase.map(attr => {
      indexedLowerCaseAttributes.getOrElse(attr, 0)
    })
    indexes.sortWith(_ < _)
  }

}


class BlackOakGoldStandardWriter {

  val conf: Config = ConfigFactory.load()

  val cleanData = "dboost.small.clean.data"
  //"data.BlackOak.clean-data-path"
  val dirtyData = "dboost.small.dirty.data"
  //"data.BlackOak.dirty-data-path"
  val outputGoldStandard = conf.getString("dboost.small.gold.log.folder") //conf.getString("output.blackouak.goldstandard")


  def createGoldStandard(): Unit = {

    val sparkSession: SparkSession = SparkSessionCreator.createSession("GOLD")



    val dirtyBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, dirtyData, BlackOakSchema.schema: _*)

    val cleanBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, cleanData, BlackOakSchema.schema: _*)

    val goldStandard: Dataset[String] = createLogGoldStandard(sparkSession, dirtyBlackOakDF, cleanBlackOakDF)


    goldStandard.write.text(outputGoldStandard)
    // always close your resources
    sparkSession.stop()
  }



  private def createLogGoldStandard(sparkSession: SparkSession, dirtyBlackOakDF: DataFrame, cleanBlackOakDF: DataFrame): Dataset[String] = {
    val schema = BlackOakSchema.schema
    val indexedAttributes = BlackOakSchema.indexedAttributes

    //here we produce log data for dirty rows and log dirty attributes:
    import sparkSession.implicits._
    val join = cleanBlackOakDF
      .joinWith(dirtyBlackOakDF, cleanBlackOakDF.col("RecID") === dirtyBlackOakDF.col("RecID"))
      .flatMap(row => {
        val cleanVals = row._1.getValuesMap[String](schema)
        val dirtyVals = row._2.getValuesMap[String](schema)

        val dirtyAttributes = for (attrName <- schema; if attrName != "RecID") yield {
          val cleanValue = cleanVals.getOrElse(attrName, "")
          val dirtyValue = dirtyVals.getOrElse(attrName, "")

          val idxIfDistinct = if (!Strings.isNullOrEmpty(cleanValue) && !cleanValue.equals(dirtyValue)) {
            s"${indexedAttributes.getOrElse(attrName, 0)}"
          } else ""
          idxIfDistinct.asInstanceOf[String]
        }
        val id = cleanVals.getOrElse("RecID", "0")
        val nonEmptyAttrs = dirtyAttributes.filter(_.nonEmpty)


        /*flatten*/
        val flattenedRow: Seq[String] = nonEmptyAttrs.map(a => s"$id,$a") /**/

        flattenedRow
      })


    join
  }


}

object BlackOakGoldStandardWriter {
  def main(args: Array[String]): Unit = {
    new BlackOakGoldStandardWriter().createGoldStandard()
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
