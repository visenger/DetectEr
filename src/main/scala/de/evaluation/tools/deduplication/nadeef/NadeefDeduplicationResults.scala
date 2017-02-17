package de.evaluation.tools.deduplication.nadeef

import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.{BlackOakSchema, HospSchema, Schema}
import de.evaluation.f1.Cells
import de.evaluation.util.{DataSetCreator, DatabaseProps, SparkLOAN}
import org.apache.spark.sql._

/**
  * Handling detected duplicates
  */
class NadeefDeduplicationResults extends Serializable {
  private var schema: Schema = null;
  private var tb_inputdb = ""
  private var recid = ""
  private var confOutputFolder = ""
  private var dedupRule = ""

  private val conf = ConfigFactory.load()


  def onSchema(s: Schema): this.type = {
    schema = s;
    recid = s.getRecID
    this
  }

  def onDirtyTable(t: String): this.type = {
    tb_inputdb = t
    this
  }

  def onRule(r: String): this.type = {
    dedupRule = r
    this
  }

  def addOutputFolder(f: String): this.type = {
    confOutputFolder = f
    this
  }

  def convertDedupResults(session: SparkSession): DataFrame = {


    val properties = DatabaseProps.getDefaultProps

    val violation = "violation"
    val violationTotal: DataFrame = session
      .read
      .jdbc(conf.getString("db.postgresql.url"), violation, properties)
    violationTotal.createOrReplaceTempView(violation)


    val dirtyData: DataFrame = session
      .read
      .jdbc(conf.getString("db.postgresql.url"), tb_inputdb, properties)
    dirtyData.createOrReplaceTempView(tb_inputdb)

    val queryDirtyDedup =
      s"""
         |SELECT v.vid, v.tupleid, i.$recid
         |FROM $violation as v
         |  JOIN $tb_inputdb as i on v.tupleid = i.tid
         |WHERE v.rid='${dedupRule}'
         |GROUP BY v.vid, v.tupleid, i.$recid
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
      // val attributes = BlackOakSchema.indexedAttributes
      val attributes = schema.indexAttributes
      val attrIds = attributes.values.toList
      val idToAttributes: List[(String, String)] = attrIds.map(i => (id, i.toString))
      idToAttributes
    })

    extendedDuplicatesIds.toDF()

  }

  //todo
  def getDedupResult(session: SparkSession, confOutputFile: String): DataFrame = {

    val dedupResults = DataSetCreator.createDataSetNoHeader(session, confOutputFile, Cells.schema: _*)
    dedupResults
  }


  def handleDuplicates(): Unit = {
    SparkLOAN.withSparkSession("DEDUP") {
      session => {
        val nadeefDedup: DataFrame = convertDedupResults(session)
        // nadeefDedup.show()

        nadeefDedup
          .coalesce(1)
          .write
          .csv(conf.getString(confOutputFolder))
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

object HospDuplicatesHandler {
  val outputFolder = "nadeef.dedup.result.folder"
  val dedupRule = "dedupHosp"

  def main(args: Array[String]): Unit = {
    val deduplicator = new NadeefDeduplicationResults()
    deduplicator.onSchema(HospSchema)
    deduplicator.onDirtyTable("tb_dirty_hosp_10k_with_rowid")
    deduplicator.onRule(dedupRule)
    deduplicator.addOutputFolder(outputFolder)
    deduplicator.handleDuplicates()

  }

  def getResults(session: SparkSession): DataFrame = {
    val hospOutput = "result.hosp.10k.deduplication"
    new NadeefDeduplicationResults().getDedupResult(session, hospOutput)
  }
}


object NadeefDeduplicationResults {

  //  def main(args: Array[String]): Unit = {
  //    new NadeefDeduplicationResults().writeResultsToDisk()
  //  }

  def convertDedupResults(session: SparkSession): DataFrame = {
    new NadeefDeduplicationResults().convertDedupResults(session)
  }

  def getDedupResults(session: SparkSession): DataFrame = {
    val confOutputFile = "output.nadeef.deduplication.result.file"
    new NadeefDeduplicationResults().getDedupResult(session, confOutputFile)
  }

}
