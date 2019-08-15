package de.evaluation.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object MemoryEstimator {

  /**
    * estimates the size of the provided dataframe in bytes
    *
    * @param df data frame, which size will be estimated
    * @return the size of dataframe in bytes
    */
  def estimate(df: DataFrame): Long = {

    import df.sparkSession.implicits._
    val rddSize: Long = df.map(row => row.mkString.getBytes().length.toLong).reduce(_ + _)

    rddSize
  }

  private def computeSizeOfString(row: String): Long = {
    row.getBytes("UTF-8").length.toLong
  }

  private def convertToRDD(df: DataFrame): RDD[String] = {

    df.rdd.map(_.toString())
  }
}
