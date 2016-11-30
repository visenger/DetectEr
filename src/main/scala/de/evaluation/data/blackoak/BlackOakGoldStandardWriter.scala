package de.evaluation.data.blackoak

import com.google.common.base.Strings
import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.util.DataSetCreator
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


  val cleanData = "data.BlackOak.clean-data-path"
  val dirtyData = "data.BlackOak.dirty-data-path"

  val conf: Config = ConfigFactory.load()


  //def main(args: Array[String]): Unit  = {
  def createGoldStandard(): Unit = {

    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("F1")
      .config("spark.local.ip",
        conf.getString("spark.config.local.ip.value"))
      .config("spark.driver.memory", "10g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()


    val dirtyBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, dirtyData, BlackOakSchema.schema: _*)

    val cleanBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, cleanData, BlackOakSchema.schema: _*)

    val goldStandard: Dataset[String] = createLogGoldStandard(sparkSession, dirtyBlackOakDF, cleanBlackOakDF)

    goldStandard.write.text(conf.getString("output.blackouak.goldstandard"))


    // always close your resources
    sparkSession.stop()
  }


  private def createDataSet(sparkSession: SparkSession, dataPathStr: String, schema: String*): DataFrame = {

    val config: Config = ConfigFactory.load()
    val dataPath: String = config.getString(dataPathStr)

    val df: DataFrame = sparkSession.read.csv(dataPath)
    val namedDF: DataFrame = df.toDF(schema: _*)

    val head: Row = namedDF.head()
    val data: DataFrame = namedDF.filter(row => row != head).toDF()
    data
  }

  private def getGoldStandard(sparkSession: SparkSession, dirtyBlackOakDF: DataFrame, cleanBlackOakDF: DataFrame): Dataset[String] = {
    import sparkSession.implicits._
    val join = cleanBlackOakDF
      .join(dirtyBlackOakDF, cleanBlackOakDF.col("RecID") === dirtyBlackOakDF.col("RecID"))
      .filter(
        cleanBlackOakDF.col("FirstName") =!= dirtyBlackOakDF.col("FirstName")
          || cleanBlackOakDF.col("MiddleName") =!= dirtyBlackOakDF.col("MiddleName")
          || cleanBlackOakDF.col("LastName") =!= dirtyBlackOakDF.col("LastName")
          || cleanBlackOakDF.col("Address") =!= dirtyBlackOakDF.col("Address")
          || cleanBlackOakDF.col("City") =!= dirtyBlackOakDF.col("City")
          || cleanBlackOakDF.col("State") =!= dirtyBlackOakDF.col("State")
          || cleanBlackOakDF.col("ZIP") =!= dirtyBlackOakDF.col("ZIP")
          || cleanBlackOakDF.col("POBox") =!= dirtyBlackOakDF.col("POBox")
          || cleanBlackOakDF.col("POCityStateZip") =!= dirtyBlackOakDF.col("POCityStateZip")
          || cleanBlackOakDF.col("SSN") =!= dirtyBlackOakDF.col("SSN")
          || cleanBlackOakDF.col("DOB") =!= dirtyBlackOakDF.col("DOB"))

    //join.show(50)

    val goldStd: DataFrame = join.select(cleanBlackOakDF.col("RecID"))
    val goldStandard: Dataset[String] = goldStd.map(row => row.getAs[String](0).trim)
    goldStandard
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
