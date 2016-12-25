package de.evaluation.tools.ruleviolations.nadeef

import java.util.Properties

import com.typesafe.config.ConfigFactory
import de.evaluation.data.blackoak.BlackOakSchema
import de.evaluation.f1.DataF1
import de.evaluation.util.{DataSetCreator, DatabaseProps}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by visenger on 23/11/16.
  */
class NadeefRulesVioResults {

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

    //joinedTabs.show(3)


    val grouped: Dataset[(Int, String, String)] = joinedTabs.map(rows => {
      val violationVals = rows._1
      val dirtyVals = rows._2

      (violationVals.getAs[Int]("tupleid"), violationVals.getAs[String]("attribute"), dirtyVals.getAs[String]("recid"))
    })
    //grouped.show(3)

    val groupedByTupleId: RDD[(String, Iterable[(Int, String, String)])] = grouped.rdd.groupBy(row => row._3)

    val dirtyRecIdAndAttributes: RDD[String] = groupedByTupleId.flatMap(entry => {
      val recid = entry._1

      val attributes: List[String] = entry._2.map(_._2).toList
      val idx: List[Int] = BlackOakSchema.getIndexesByAttrNames(attributes)
      val flattenIdx: List[String] = idx.map(id => s"$recid,$id")

      flattenIdx
    })


    val f1: Dataset[String] = dirtyRecIdAndAttributes.toDS()

    f1.write.text(conf.getString("output.nadeef.detect.result.folder"))

    sparkSession.stop()
  }

  def getRulesVioResults(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val nadeefResultConf = "output.nadeef.detect.result.file"
    val nadeefResult: DataFrame = DataSetCreator
      .createDataSetNoHeader(sparkSession, nadeefResultConf, DataF1.schema: _*)

    nadeefResult
  }

}

object NadeefRulesVioResults {

  def main(args: Array[String]): Unit = {
    new NadeefRulesVioResults().evaluate()
  }

  def getRulesVioResults(sparkSession: SparkSession): DataFrame = {
    new NadeefRulesVioResults().getRulesVioResults(sparkSession)
  }

}



