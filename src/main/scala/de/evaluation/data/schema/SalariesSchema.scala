package de.evaluation.data.schema

/**
  * Created by visenger on 10/02/17.
  */
object SalariesSchema extends Schema {

  val recid = "oid"
  val schema = Seq(recid, "id", "employeename", "jobtitle", "basepay", "overtimepay", "otherpay", "benefits", "totalpay", "totalpaybenefits", "year", "notes", "agency", "status")

  override def getSchema: Seq[String] = schema

  override def getRecID: String = recid
}
