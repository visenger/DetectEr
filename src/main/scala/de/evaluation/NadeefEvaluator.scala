package de.evaluation

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

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

    import sparkSession.implicits._

    val props: Properties = new Properties()
    props.put("user", conf.getString("db.postgresql.user"))
    props.put("password", conf.getString("db.postgresql.password"))
    props.put("driver", conf.getString("db.postgresql.driver"))

    val violation: DataFrame = sparkSession
      .read
      .jdbc(conf.getString("db.postgresql.url"), "violation", props)

    val dirtyTable: DataFrame = sparkSession.read.jdbc(conf.getString("db.postgresql.url"), "tb_inputdb", props)


    val violationColumns: DataFrame = violation
      .select(violation.col("tupleid"), violation.col("attribute"), violation.col("value"))
      .dropDuplicates()

    val dirtyTabColumns: DataFrame = dirtyTable
      .select(dirtyTable.col("tid"), dirtyTable.col("recid"))

    val joinedTabs: Dataset[(Row, Row)] = violationColumns
      .joinWith(dirtyTabColumns, violationColumns.col("tupleid") === dirtyTabColumns.col("tid"))

    joinedTabs.show(3)


    val grouped: Dataset[(Int, String, String)] = joinedTabs.map(rows => {
      val violationVals = rows._1
      val dirtyVals = rows._2

      (violationVals.getAs[Int]("tupleid"), violationVals.getAs[String]("attribute"), dirtyVals.getAs[String]("recid"))
    })
    grouped.show(3)

    val groupedByTupleId: RDD[(String, Iterable[(Int, String, String)])] = grouped.rdd.groupBy(row => row._3)

    val dirtyRecIdAndAttributes: RDD[String] = groupedByTupleId.map(entry => {
      val recid = entry._1
      val attributes: List[String] = entry._2.map(_._2).toList
      val attsToString = attributes.mkString(",")

      s"$recid,$attsToString"
    })






    val f1: Dataset[String] = dirtyRecIdAndAttributes.toDS()
    f1.show(34)


    //violation.show(2)

    val dirtyDataSchema = dirtyTable.schema

    dirtyDataSchema.fields
      .foreach(f => println(s"field name: ${f.name}, data type: ${f.dataType}, metadata= ${f.metadata}"))

    println()
    val schema = violation.schema

    schema.fields
      .foreach(f => println(s"field name: ${f.name}, data type: ${f.dataType}, metadata= ${f.metadata}"))


    sparkSession.stop()
  }

}

object NadeefEvaluator {

  def main(args: Array[String]): Unit = {
    new NadeefEvaluator().evaluate()
  }

}



