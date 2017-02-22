package de.evaluation.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by visenger on 29/11/16.
  */
object DataSetCreator {


  def createDataSet(sparkSession: SparkSession, dataPathStr: String, schema: String*): DataFrame = {

    val config: Config = ConfigFactory.load()
    val dataPath: String = config.getString(dataPathStr)

    val data: DataFrame = createDataSetFromFile(sparkSession, dataPath, schema: _*)
    data
  }

  def createDataSetFromFile(sparkSession: SparkSession, dataPath: String, schema: String*): DataFrame = {
    val namedDF: DataFrame = createDataSetFromFileNoHeader(sparkSession, dataPath, schema: _*)

    val head: Row = namedDF.head()

    val data: DataFrame = namedDF.filter(row => row != head).toDF()
    data
  }

  def createDataSetFromCSV(sparkSession: SparkSession, dataPathStr: String, schema: String*): DataFrame = {

    /*
    * todo: consider the following way of reading csv:
    * val csvdata = spark.read.options(Map(
    "header" -> "true",
    "ignoreLeadingWhiteSpace" -> "true",
    "ignoreTrailingWhiteSpace" -> "true",
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss.SSSZZZ",
    "inferSchema" -> "true",
    "mode" -> "FAILFAST"))
  .csv("s3a://landsat-pds/scene_list.gz")

    * */

    val config: Config = ConfigFactory.load()
    val dataPath: String = config.getString(dataPathStr)

    val csv = sparkSession
      .read
      .option("header", "true")
      .csv(dataPath)

    val df = csv.toDF(schema: _*)
    df

  }

  @Deprecated //todo: use createDataSetFromCSV method.
  def createDataSetNoHeader(sparkSession: SparkSession, dataPathStr: String, schema: String*): DataFrame = {

    val config: Config = ConfigFactory.load()
    val dataPath: String = config.getString(dataPathStr)

    val namedDF: DataFrame = createDataSetFromFileNoHeader(sparkSession, dataPath, schema: _*)
    namedDF
  }


  def createDataSetFromFileNoHeader(sparkSession: SparkSession, dataPath: String, schema: String*): DataFrame = {
    val df: DataFrame = sparkSession.read.csv(dataPath)
    val namedDF: DataFrame = df.toDF(schema: _*)
    namedDF
  }

  @Deprecated
  def createDataSetWithoutFirstTwo(sparkSession: SparkSession, dataPathStr: String, schema: String*): DataFrame = {
    import sys.process._

    val config: Config = ConfigFactory.load()
    val dataPath: String = config.getString(dataPathStr)

    val executeSEDCommand = s"sed -i -e  1,2d  $dataPath" ! //KISS ;) removing first two lines from provided file

    val csvDF: DataFrame = if (executeSEDCommand == 0) {
      val df: DataFrame = sparkSession.read.csv(dataPath).toDF(schema: _*)
      df
    } else {
      sparkSession.emptyDataFrame
    }
    csvDF

  }

}
