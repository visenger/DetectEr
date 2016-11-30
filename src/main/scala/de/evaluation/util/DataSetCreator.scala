package de.evaluation.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by visenger on 29/11/16.
  */
object DataSetCreator {


  def createDataSet(sparkSession: SparkSession, dataPathStr: String, schema: String*): DataFrame = {

    val config: Config = ConfigFactory.load()
    val dataPath: String = config.getString(dataPathStr)

    val df: DataFrame = sparkSession.read.csv(dataPath)
    val namedDF: DataFrame = df.toDF(schema: _*)

    val head: Row = namedDF.head()
    val data: DataFrame = namedDF.filter(row => row != head).toDF()
    data
  }

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
