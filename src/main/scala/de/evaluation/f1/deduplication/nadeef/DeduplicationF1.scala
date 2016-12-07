package de.evaluation.f1.deduplication.nadeef

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.F1
import de.evaluation.util.{DatabaseProps, SparkSessionCreator}
import org.apache.spark.sql._


class DeduplicationF1 {

  val conf = ConfigFactory.load()

  def evaluate(): Unit = {

    val session = SparkSessionCreator.createSession("DEDUP-NADEEF")

    val properties = DatabaseProps.getDefaultProps

    val violation = "violation"
    val violationTotal: DataFrame = session
      .read
      .jdbc(conf.getString("db.postgresql.url"), violation, properties)
    violationTotal.createOrReplaceTempView(violation)

    val tb_inputdb = "tb_inputdb"
    val dirtyData: DataFrame = session
      .read
      .jdbc(conf.getString("db.postgresql.url"), tb_inputdb, properties)
    dirtyData.createOrReplaceTempView(tb_inputdb)


    val queryDirtyDedup =
      s"""
         |SELECT v.vid, v.tupleid, i.recid
         |FROM $violation as v
         |  JOIN $tb_inputdb as i on v.tupleid = i.tid
         |WHERE v.rid='${conf.getString("deduplication.rule.for.dirty.data")}'
         |GROUP BY v.vid, v.tupleid, i.recid
       """.stripMargin


    val dirtyDuplicates: Dataset[List[String]] = getDuplicatePairs(session, queryDirtyDedup)
    //    dirtyDuplicates.show(5)
    //    val countDirtyDupli = dirtyDuplicates.count()

    /** *********************************************/


    val tb_grounddb = "tb_grounddb"
    val cleanData: DataFrame = session
      .read
      .jdbc(conf.getString("db.postgresql.url"), tb_grounddb, properties)
    cleanData.createOrReplaceTempView(tb_grounddb)

    val queryGoldDedup =
      s"""
         |SELECT v.vid, v.tupleid, i.recid
         |FROM $violation as v
         |  JOIN $tb_grounddb as i on v.tupleid = i.tid
         |WHERE v.rid='${conf.getString("deduplication.rule.for.gold.data")}'
         |GROUP BY v.vid, v.tupleid, i.recid
         """.stripMargin

    val goldDuplicates: Dataset[List[String]] = getDuplicatePairs(session, queryGoldDedup)

    val dedupResult = F1.evaluateResult(goldDuplicates.toDF(), dirtyDuplicates.toDF())
    dedupResult.printResult("deduplication")

    session.stop()

  }

  private def getDuplicatePairs(session: SparkSession, query: String): Dataset[List[String]] = {
    import session.implicits._
    val dedupDirtyData: DataFrame = session.sql(query)
    val dirtyGroupedByVID: KeyValueGroupedDataset[Int, Row] = dedupDirtyData.groupByKey(row => row.getAs[Int](0))
    val dirtyDuplicates: Dataset[List[String]] = dirtyGroupedByVID.mapGroups((k, v) => {
      val duplicates: List[String] = v.map(row => row.getAs[String](2)).toList
      duplicates

    })
    dirtyDuplicates
  }
}

object DeduplicationF1 {
  def main(args: Array[String]): Unit = {
    new DeduplicationF1().evaluate()
  }
}
