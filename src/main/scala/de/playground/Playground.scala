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

  /**
    *
    * (exists-2,-0.27)
    * (exists-3,-0.17)
    * (intercept,-0.64)
    * (trainR,0.3811)
    * (exists-5,0.76)
    * (exists-1,0.15)
    * (trainF1,0.4425)
    * (threshold,0.4176)
    * (trainP,0.5276)
    * (exists-4,-0.17)
    **/

  println("-0.27".toDouble)

}


object StatPlayground {
  def main(args: Array[String]): Unit = {

  }
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

  import de.model.util.NumbersUtil.round

  val prTuples: Seq[(Double, Double)] = Seq((0.81, 0.97))

  prTuples.foreach(tuple => {
    val f1 = 2 * (tuple._1.toDouble * tuple._2.toDouble) / (tuple._1 + tuple._2)

    println(s"Precision: ${round(tuple._1)}, Recall: ${round(tuple._2)}, F1: ${round(f1, 4)}")

  })
}


