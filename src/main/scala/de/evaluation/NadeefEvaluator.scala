package de.evaluation

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by visenger on 23/11/16.
  */
class NadeefEvaluator {

  val conf = ConfigFactory.load()

  def evaluate(): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("NADEEF-EVAL")
      .config("spark.driver.memory", "10g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()


    val props: Properties = new Properties()
    props.put("user", conf.getString("db.postgresql.user"))
    props.put("password", conf.getString("db.postgresql.password"))
    props.put("driver", conf.getString("db.postgresql.driver"))

    val table: DataFrame = sparkSession
      .read
      .jdbc(conf.getString("db.postgresql.url"), "violation", props)


    table.show(2)

    val schema = table.schema

    schema.fields.foreach(f => println(s"field name: ${f.name}, data type: ${f.dataType}, metadata= ${f.metadata}"))


    sparkSession.stop()
  }

}

object NadeefEvaluator {

  def main(args: Array[String]): Unit = {
    new NadeefEvaluator().evaluate()
  }

}



