package de.evaluation.data.util

import org.apache.spark.sql.DataFrame

object WriterUtil {

  def persistTSVWithoutHeader(dataFrame: DataFrame, pathToWrite: String): Unit = {
    dataFrame
      .repartition(1)
      .write
      .option("sep", "\\t")
      .option("header", "false")
      .csv(pathToWrite)
  }

  def persistCSVWithoutHeader(dataFrame: DataFrame, pathToWrite: String): Unit = {
    dataFrame
      .repartition(1)
      .write
      .option("header", "false")
      .csv(pathToWrite)
  }

  def persistCSV(dataFrame: DataFrame, pathToWrite: String): Unit = {
    dataFrame
      .repartition(1)
      .write
      .option("header", "true")
      .csv(pathToWrite)
  }

}
