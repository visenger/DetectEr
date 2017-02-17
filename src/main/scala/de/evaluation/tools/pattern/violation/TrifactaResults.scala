package de.evaluation.tools.pattern.violation

import com.google.common.base.Strings
import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.{BlackOakSchema, HospSchema, SalariesSchema}
import de.evaluation.f1.Cells
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Handling pattern violation results.
  *
  * see src/scripts/trifacta.script.txt in order to understand the
  * result provided by trifacta tool.
  */

trait TrifactaSchema extends Serializable {
  def getRecId: String

  def getPrefix: String

  def getSchema: Seq[String]

  def getAttributesIdx(): Map[String, Int]
}

object TrifactaSalariesSchema extends TrifactaSchema {

  private val recid = "oid"
  private val prefix = "vio_"
  private val schema = Seq(recid, "employeename", s"${prefix}employeename", "jobtitle", s"${prefix}jobtitle", "basepay", s"${prefix}basepay", "benefits", s"${prefix}benefits", "totalpay", s"${prefix}totalpay", "status", s"${prefix}status")
  private val salariesAttrs = Seq(recid, "employeename", "jobtitle", "basepay", "benefits", "totalpay", "status")

  //
  override def getRecId: String = recid

  override def getPrefix: String = prefix

  override def getSchema: Seq[String] = schema

  override def getAttributesIdx(): Map[String, Int] = {
    val salariesAttributes = SalariesSchema.indexAttributes
    salariesAttrs.map(attr => {
      val idx = salariesAttributes.getOrElse(attr, 0)
      attr -> idx
    }).toMap
  }
}

object TrifactaHospSchema extends TrifactaSchema {
  private val recid = "oid"
  private val prefix = "ismismatched_"
  private val schema = Seq(recid, "prno", s"${prefix}prno", "zip", s"${prefix}zip", "phone", s"${prefix}phone")
  private val hospAttrs = Seq("prno", "zip", "phone")


  override def getAttributesIdx(): Map[String, Int] = {
    val hospAttributes = HospSchema.indexAttributes
    hospAttrs.map(attr => {
      val idx = hospAttributes.getOrElse(attr, 0)
      attr -> idx
    }).toMap
  }

  override def getSchema: Seq[String] = schema

  override def getRecId: String = recid

  override def getPrefix: String = prefix
}

object TrifactaBlackOackSchema extends TrifactaSchema {
  //TODO: finish!
  override def getAttributesIdx(): Map[String, Int] = null

  override def getSchema: Seq[String] = Nil

  override def getRecId: String = BlackOakSchema.getRecID

  override def getPrefix: String = ""
}


class TrifactaResults {


  val conf = ConfigFactory.load()
  private var trifactaData = ""
  private var outputFolder = ""
  private var schema: TrifactaSchema = null

  def onSchema(s: TrifactaSchema): this.type = {
    schema = s
    this
  }

  def onTrifactaResult(file: String): this.type = {
    trifactaData = file
    this
  }

  def addOutputFolder(f: String): this.type = {
    outputFolder = f
    this
  }


  def writePatternVioLog(): Unit = {
    SparkLOAN.withSparkSession("PATTERNVIO") {
      session => {
        val patternVioResult: DataFrame = DataSetCreator.createDataSetNoHeader(session, trifactaData, schema.getSchema: _*)
        val converted = convertPatternViolationResult(session, patternVioResult)
        converted.show()
        converted
          .coalesce(1)
          .write
          .text(conf.getString(outputFolder))
      }
    }
  }

  private def convertPatternViolationResult(session: SparkSession, patternVioResult: DataFrame): DataFrame = {
    import session.implicits._

    //always extract defaults outside rdd operators
    val default = 0
    val currentSchema = schema.getSchema
    val recIdOfSchema = schema.getRecId
    val prefix = schema.getPrefix
    val schemaAttributes = schema.getAttributesIdx()

    val convertedPatternVio: RDD[String] = patternVioResult.rdd.flatMap(row => {

      val rowValuesMap: Map[String, String] = row.getValuesMap(currentSchema)

      val recId = rowValuesMap.getOrElse(recIdOfSchema, "")

      val violations = rowValuesMap.partition(entry => {
        val value = entry._2
        !Strings.nullToEmpty(value).isEmpty &&
          value.equalsIgnoreCase("true")
      })._1

      val allAttrsIdx: Set[Int] = violations.keySet.map(k => {

        val attributeIdx: Int = k.startsWith(prefix) match {
          case true => {
            val keyWithoutPrefix = k.replaceFirst(prefix, "")
            val attrIdx = schemaAttributes.getOrElse(keyWithoutPrefix, default)
            attrIdx
          }
          case false => default
        }
        attributeIdx
      })

      val convertedRow: Set[String] = allAttrsIdx
        .filterNot(_ == default)
        .map(attr => s"$recId,$attr")

      convertedRow
    })

    convertedPatternVio.toDF()

  }


  def getPatternViolationResult(session: SparkSession, file: String): DataFrame = {

    val trifactaOutput = DataSetCreator.createDataSetNoHeader(session, file, Cells.schema: _*)
    trifactaOutput
  }


}

object TrifactaSalariesResults {
  def main(args: Array[String]): Unit = {
    val result = "trifacta.salaries.vio"
    val outputFolder = "trifacta.salaries.result.folder"

    val sTrifacta = new TrifactaResults()
    sTrifacta.onSchema(TrifactaSalariesSchema)
    sTrifacta.onTrifactaResult(result)
    sTrifacta.addOutputFolder(outputFolder)
    sTrifacta.writePatternVioLog()

  }

  def getResult(session: SparkSession): DataFrame = {
    val confString = "result.salaries.pattern.vio"
    val trifactaOutput = DataSetCreator.createDataSetNoHeader(session, confString, Cells.schema: _*)
    trifactaOutput
  }
}

object TrifactaHospResults {
  def main(args: Array[String]): Unit = {

    val result = "trifacta.hosp.vio"
    val outputFolder = "trifacta.hosp.result.folder"

    val trifacta = new TrifactaResults()
    trifacta.onSchema(TrifactaHospSchema)
    trifacta.onTrifactaResult(result)
    trifacta.addOutputFolder(outputFolder)
    trifacta.writePatternVioLog()
  }

  def getResult(session: SparkSession): DataFrame = {
    val confString = "result.hosp.10k.pattern.vio"
    val trifactaOutput = DataSetCreator.createDataSetNoHeader(session, confString, Cells.schema: _*)
    trifactaOutput
  }


}

object TrifactaBlackOakReslults {

  def getPatternViolationResult(session: SparkSession): DataFrame = {
    val confString = "output.trifacta.result.file"
    new TrifactaResults().getPatternViolationResult(session, confString)
  }
}

object TrifactaResults {


  def getPatternViolationResult(session: SparkSession): DataFrame = {
    val confString = "output.trifacta.result.file"
    new TrifactaResults().getPatternViolationResult(session, confString)
  }


}
