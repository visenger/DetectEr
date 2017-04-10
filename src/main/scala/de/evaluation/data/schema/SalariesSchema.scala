package de.evaluation.data.schema

/**
  * Created by visenger on 10/02/17.
  */
object SalariesSchema extends Schema with AttributesDataType {

  val recid = "oid"
  private val id = "id"
  private val employeename = "employeename"
  private val jobtitle = "jobtitle"
  private val basepay = "basepay"
  private val overtimepay = "overtimepay"
  private val otherpay = "otherpay"
  private val benefits = "benefits"
  private val totalpay = "totalpay"
  private val totalpaybenefits = "totalpaybenefits"
  private val year = "year"
  private val notes = "notes"
  private val agency = "agency"
  private val status = "status"
  val schema = Seq(recid, id, employeename, jobtitle, basepay, overtimepay,
    otherpay, benefits, totalpay, totalpaybenefits, year, notes, agency, status)

  override def getSchema: Seq[String] = schema

  override def getRecID: String = recid

  override def dataTypesDictionary: Map[String, String] = Map(
    recid -> integer,
    id -> integer,
    employeename -> string,
    jobtitle -> string,
    basepay -> decimal,
    overtimepay -> decimal,
    otherpay -> decimal,
    benefits -> decimal,
    totalpay -> decimal,
    totalpaybenefits -> decimal,
    year -> date,
    notes -> string,
    agency -> string,
    status -> string
  )
}
