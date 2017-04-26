package de.evaluation.tools.ruleviolations.nadeef

import java.util.Properties

import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema._
import de.evaluation.f1.Cells
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by visenger on 23/11/16.
  */
class NadeefRulesVioResults extends Serializable {

  //subject of change:
  private var dirtyInput = ""
  /*"tb_dirty_hosp_10k_with_rowid" -> this is a name of the input(dirty) table created by Nadeef.*/
  private var recId = ""
  private var schema: Schema = null
  private var output = ""

  //generalized:
  private val conf = ConfigFactory.load()

  private val props: Properties = new Properties()
  props.put("user", conf.getString("db.postgresql.user"))
  props.put("password", conf.getString("db.postgresql.password"))
  props.put("driver", conf.getString("db.postgresql.driver"))

  private val url = conf.getString("db.postgresql.url")

  //Schema:
  //VIOLATION (vid,rid,tablename,tupleid,attribute,value)
  private val violationTable = "violation"
  private val attribute = "attribute"
  private val tupleid = "tupleid"
  private val value = "value"
  private val dirtyTupleId = "tid"

  def onSchema(s: Schema): this.type = {
    schema = s
    recId = s.getRecID
    this
  }

  def addDirtyTableName(table: String): this.type = {
    dirtyInput = table
    this
  }

  def specifyOutput(out: String): this.type = {
    output = out
    this
  }


  def createRulesVioLog(): Unit = {
    SparkLOAN.withSparkSession("RULESVIO") {
      session => {

        import session.implicits._

        val violation: DataFrame = session
          .read
          .jdbc(url, violationTable, props)

        val dirtyTable: DataFrame = session.read.jdbc(url, dirtyInput, props)

        val violationColumns: DataFrame = violation
          .select(violation.col(tupleid), violation.col(attribute), violation.col(value))
          .dropDuplicates()

        val dirtyTabColumns: DataFrame = dirtyTable
          .select(dirtyTable.col(dirtyTupleId), dirtyTable.col(recId))

        val joinedTabs: Dataset[(Row, Row)] = violationColumns
          .joinWith(dirtyTabColumns, violationColumns.col(tupleid) === dirtyTabColumns.col(dirtyTupleId))

        val grouped: Dataset[(Int, String, String)] = joinedTabs.map(rows => {
          val violationVals = rows._1
          val dirtyVals = rows._2

          /*val attribute = "attribute"
          val tupleid = "tupleid"
          val recId = "oid"*/

          //todo: recId should be string

          (violationVals.getAs[Int](tupleid), violationVals.getAs[String](attribute), dirtyVals.getAs[String](recId))
        })

        val groupedByTupleId: RDD[(String, Iterable[(Int, String, String)])] = grouped.rdd.groupBy(row => row._3)

        val dirtyRecIdAndAttributes: RDD[String] = groupedByTupleId.flatMap(entry => {

          val recid = entry._1
          val attributes: List[String] = entry._2.map(_._2).toList
          val idx: List[Int] = schema.getIndexesByAttrNames(attributes)
          val flattenIdx: List[String] = idx.map(id => s"$recid,$id")

          flattenIdx
        })

        val detectedCells = dirtyRecIdAndAttributes.toDS()
        detectedCells.show()

        detectedCells
          .coalesce(1)
          .write
          .text(conf.getString(output))
      }
    }
  }

  @Deprecated
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
      .select(dirtyTable.col("tid"), dirtyTable.col(recId))

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

    // f1.write.text(conf.getString("output.nadeef.detect.result.folder"))

    sparkSession.stop()
  }

  def getRulesVioResults(sparkSession: SparkSession): DataFrame = {

    val nadeefResultConf = "output.nadeef.detect.result.file"
    val nadeefResult: DataFrame = DataSetCreator
      .createDataSetNoHeader(sparkSession, nadeefResultConf, Cells.schema: _*)

    nadeefResult
  }

}

object HospRulesVioRunner {
  def main(args: Array[String]): Unit = {
    val rulesVioResults = new NadeefRulesVioResults()
    rulesVioResults.onSchema(HospSchema)

    rulesVioResults.addDirtyTableName("tb_dirty_hosp_10k_with_rowid")
    rulesVioResults.specifyOutput("nadeef.rules.vio.result.folder")
    rulesVioResults.createRulesVioLog()
  }

  def getResult(session: SparkSession): DataFrame = {
    val confString = "result.hosp.10k.rules.vio"
    val rulesVioOutput = DataSetCreator.createDataSetNoHeader(session, confString, Cells.schema: _*)
    rulesVioOutput
  }

}

object FlightsRulesVioRunner {
  def main(args: Array[String]): Unit = {
    val dirtyTabName = "tb_flights_dirty"
    val outputFolder = "nadeef.flights.rules.vio.result.folder"

    val rulesVioRunner = new NadeefRulesVioResults()
    rulesVioRunner.onSchema(FlightsSchema)
    rulesVioRunner.addDirtyTableName(dirtyTabName)
    rulesVioRunner.specifyOutput(outputFolder)
    rulesVioRunner.createRulesVioLog()
  }

  def getResult(session: SparkSession): DataFrame = {
    val confString = "result.flights.rules.vio"
    val rulesVioOutput = DataSetCreator.createDataSetNoHeader(session, confString, Cells.schema: _*)
    rulesVioOutput
  }
}

object SalariesRulesVioRunner {
  def main(args: Array[String]): Unit = {
    val dirtyTableName = "tb_dirty_salaries_with_id_nadeef_version"

    val rulesVioResults = new NadeefRulesVioResults()
    rulesVioResults.onSchema(SalariesSchema)
    rulesVioResults.addDirtyTableName(dirtyTableName)
    rulesVioResults.specifyOutput("nadeef.rules.vio.result.folder")
    rulesVioResults.createRulesVioLog()

  }

  def getResult(session: SparkSession): DataFrame = {
    val confString = "result.salaries.rules.vio"
    val rulesVioOutput = DataSetCreator.createDataSetNoHeader(session, confString, Cells.schema: _*)
    rulesVioOutput
  }
}


object NadeefRulesVioResults {

  //  def main(args: Array[String]): Unit = {
  //    // new NadeefRulesVioResults().evaluate()
  //    new NadeefRulesVioResults().createRulesVioLog()
  //  }

  def getRulesVioResults(sparkSession: SparkSession): DataFrame = {
    new NadeefRulesVioResults().getRulesVioResults(sparkSession)
  }

}



