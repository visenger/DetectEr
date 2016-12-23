package de.model.util

import com.typesafe.config.ConfigFactory
import de.evaluation.data.blackoak.BlackOakSchema
import de.evaluation.util.{DataSetCreator, DatabaseProps, SparkSessionCreator}
import org.apache.spark.sql._

/**
  * Created by visenger on 22/12/16.
  */
class ResultCruncher {

  private val conf = ConfigFactory.load()
  private val cleanData = "data.BlackOak.clean-data-path"
  private val dirtyData = "data.BlackOak.dirty-data-path"
  private val trifactaData = "trifacta.cleaning.result"

  def prepareDedupResults(): Unit = {

    val session = SparkSessionCreator.createSession("DEDUP")

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

    val extendedDuplicatesIds: Dataset[String] = distinctIds.flatMap(id => {
      val blackOakAttrIds = BlackOakSchema.indexedAttributes.values.toList
      val idToAttributes: List[String] = blackOakAttrIds.map(i => s"$id,$i").toList
      idToAttributes
    })
    /**
      * todo steps: write extended duplicates into a file.
      **/
    extendedDuplicatesIds.show(5)

    session.stop()
  }

  def preparePatternVioResults(): Unit = {


    val sparkSession: SparkSession = SparkSessionCreator.createSession("TFCT")

    import scala.collection.JavaConversions._
    import sparkSession.implicits._


    val attributesNames = conf.getStringList("trifacta.fields").toList
    /** read attribute names from config file   */

    /*what was cleaned*/
    val trifactaDF: DataFrame = DataSetCreator
      .createDataSet(sparkSession, trifactaData, BlackOakSchema.schema: _*)
    val trifactaCols: List[Column] = getColumns(trifactaDF, attributesNames)
    val trifactaProjection: DataFrame = trifactaDF.select(trifactaCols: _*)

    /*what was dirty*/
    val dirtyBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, dirtyData, BlackOakSchema.schema: _*)
    val dirtyDFCols: List[Column] = getColumns(dirtyBlackOakDF, attributesNames)
    val dirtyDataProjection = dirtyBlackOakDF.select(dirtyDFCols: _*)


    val whatTrifactaFound = dirtyDataProjection.except(trifactaProjection)

    val recid = "RecID"
    val nonIdAttributes = attributesNames.diff(Seq(recid))
    val idxOfAttributes: List[Int] = nonIdAttributes
      .map(attr => BlackOakSchema.indexedLowerCaseAttributes.getOrElse(attr.toLowerCase, 0))

    val extendedPatternVioFields: Dataset[String] = whatTrifactaFound.toDF().flatMap(row => {
      val id = row.getAs[String](recid)
      val foundFields: List[String] = idxOfAttributes.map(attrIdx => s"$id,$attrIdx")
      foundFields
    })

    /** todo: write extended duplicates into a file */
    extendedPatternVioFields.show(6)

    sparkSession.stop()

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

  private def getColumns(df: DataFrame, attributesNames: List[String]) = {
    attributesNames.map(attr => df.col(attr))
  }

}

object ResultCruncher {
  def main(args: Array[String]): Unit = {
    new ResultCruncher().preparePatternVioResults()
  }

}
