package de.evaluation.data

import com.google.common.base.Strings
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by visenger on 27/11/16.
  */
class BlackOak {


}

object BlackOak {


  val cleanData = "data.BlackOak.clean-data-path"
  val dirtyData = "data.BlackOak.dirty-data-path"

  val config: Config = ConfigFactory.load()
  val schema = Seq("RecID", "FirstName", "MiddleName", "LastName", "Address", "City", "State", "ZIP", "POBox", "POCityStateZip", "SSN", "DOB")

  val indexedAttributes: Map[String, Int] = schema.zipWithIndex.toMap.map(
    e => (e._1, {
      var i = e._2;
      i += 1; // 1-based indexing
      i
    }))

  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("F1")
      .config("spark.local.ip",
        config.getString("spark.config.local.ip.value"))
      .config("spark.driver.memory", "10g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()


    import sparkSession.implicits._


    val indexedFields = schema.zipWithIndex.toMap
    val dirtyBlackOakDF: DataFrame = createDataSet(sparkSession, dirtyData, schema: _*)
    //dirtyBlackOakDF.createOrReplaceTempView("dirtyDB")
    //dirtyBlackOakDF.show(20)


    val cleanBlackOakDF: DataFrame = createDataSet(sparkSession, cleanData, schema: _*)

    val goldStandard: Dataset[String] = createLogGoldStandard(sparkSession, dirtyBlackOakDF, cleanBlackOakDF)


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

    //here we produce log data for dirty rows and log dirty attributes:
    import sparkSession.implicits._
    val join = cleanBlackOakDF
      .joinWith(dirtyBlackOakDF, cleanBlackOakDF.col("RecID") === dirtyBlackOakDF.col("RecID"))
      .map(row => {
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

        val logLine = s"""${cleanVals.getOrElse("RecID", "0")}, ${dirtyAttributes.filter(_.nonEmpty).mkString(",")}"""

        val clean = cleanVals.mkString("[", " , ", "]")
        val dirty = dirtyVals.mkString("{", " , ", "}")

        val str = s"log: $logLine clean: $clean dirty: $dirty"
        str
      })

    val line = join.take(1)
    line.foreach(println)


    /*.filter(
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
*/
    //join.show(50)

    //    val goldStd: DataFrame = join.select(cleanBlackOakDF.col("RecID"))
    //    val goldStandard: Dataset[String] = goldStd.map(row => row.getAs[String](0).trim)
    //    goldStandard
    join
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
