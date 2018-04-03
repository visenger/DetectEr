package de.evaluation.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

/**
  * Created by visenger on 30/11/16.
  */
object SparkSessionCreator {
  private val conf: Config = ConfigFactory.load()

  def createSession(appName: String) = {
    SparkSession
      .builder()
      .master("local[8]")
      .appName(appName)
      .config("spark.local.ip",
        conf.getString("spark.config.local.ip.value"))
      .config("spark.driver.memory", "10g")
      .config("spark.executor.memory", "8g")
      //.config("spark.sql.crossJoin.enabled", "true")
      .config("spark.driver.host", "localhost") //had to add this setting due to the error "SparkContext: Error initializing SparkContext.    java.lang.AssertionError: assertion failed: Expected hostname"
      //solution found at https://stackoverflow.com/questions/34601554/mac-spark-shell-error-initializing-sparkcontext/34858724
      .getOrCreate()
  }

}
