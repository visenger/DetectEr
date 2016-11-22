package de.playground

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by visenger on 16/11/16.
  */
object Playground extends App {

  println("hello world!")

  private val config: Config = ConfigFactory.load()
  private val path: String = config.getString("data.BlackOak.clean-data-path")
  println("path = " + path)

}

case class Record(key: Int, value: String)

object SparkPlayground {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("spark-sql-playground")
      .master("local[4]")
      .getOrCreate()

    // import session.implicits._

    val df: DataFrame = session.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    df.createOrReplaceTempView("records")

    println("result of the sql query:")

    session.sql("select * from records where key > 88").collect().foreach(println)

    //session.read.csv()

    session.stop();

  }


}
