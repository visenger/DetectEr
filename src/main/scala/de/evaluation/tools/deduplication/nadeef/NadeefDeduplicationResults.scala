package de.evaluation.tools.deduplication.nadeef

import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.BlackOakSchema
import de.evaluation.f1.DataF1
import de.evaluation.util.{DataSetCreator, DatabaseProps, SparkLOAN}
import org.apache.spark.sql._

/**
  * Created by visenger on 25/12/16.
  */
class NadeefDeduplicationResults {
  private val conf = ConfigFactory.load()
  private val confOutputFolder = "output.nadeef.deduplication.result.folder"
  private val confOutputFile = "output.nadeef.deduplication.result.file"

  def convertDedupResults(session: SparkSession): DataFrame = {

    // val session = SparkSessionCreator.createSession("DEDUP")

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

    /**
      * Assuming that duplicates are data points represented by the complete line,
      * we include all attributes as found values.
      * todo steps:
      * 1. get distinct ids
      * 2. for each id create list: id, attr#
      **/
    import session.implicits._
    val distinctIds: Dataset[String] = dirtyDuplicates
      .flatMap(list => list)
      .distinct()

    val extendedDuplicatesIds: Dataset[(String, String)] = distinctIds.flatMap(id => {
      val blackOakAttrIds = BlackOakSchema.indexedAttributes.values.toList
      val idToAttributes: List[(String, String)] = blackOakAttrIds.map(i => (id, i.toString))
      idToAttributes
    })
    /**
      * todo steps: write extended duplicates into a file.
      **/
    extendedDuplicatesIds.toDF(DataF1.schema: _*)

    // session.stop()
  }

  def getDedupResult(session: SparkSession): DataFrame = {
    val dedupResults = DataSetCreator.createDataSetNoHeader(session, confOutputFile, DataF1.schema: _*)
    dedupResults
  }


  def writeResultsToDisk(): Unit = {
    SparkLOAN.withSparkSession("ndf") {
      session => {
        val nadeefDedup: DataFrame = convertDedupResults(session)

        nadeefDedup.write.csv(conf.getString(confOutputFolder))
      }
    }
  }

  private def getDuplicatePairs(session: SparkSession, query: String): Dataset[List[String]] = {

    /**
      * the result is of the form:
      * +------------------+
      * |             value|
      * +------------------+
      * |[A961269, A992384]|
      * |[A939682, A939624]|
      * |[A990110, A967936]|
      * +------------------+
      *
      **/

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

object NadeefDeduplicationResults {

  def main(args: Array[String]): Unit = {
    new NadeefDeduplicationResults().writeResultsToDisk()
  }

  def convertDedupResults(session: SparkSession): DataFrame = {
    new NadeefDeduplicationResults().convertDedupResults(session)
  }

  def getDedupResults(session: SparkSession): DataFrame = {
    new NadeefDeduplicationResults().getDedupResult(session)
  }

}
