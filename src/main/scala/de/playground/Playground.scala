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


object F1Calculator extends App {

  val prTuples: Seq[(Double, Double)] = Seq((0.5678, 0.3117), (0.5404, 0.2352), (0.7335, 0.4531), (0.6655, 0.3976))

  prTuples.foreach(tuple => {
    val f1 = 2 * (tuple._1.toDouble * tuple._2.toDouble) / (tuple._1 + tuple._2)

    println(s"Precision: ${tuple._1}, Recall: ${tuple._2}, F1: ${f1}")

  })
}
