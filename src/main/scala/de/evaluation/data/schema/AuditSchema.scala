package de.evaluation.data.schema

object AuditSchema {

  private val id = "id"
  private val vid = "vid"
  private val tupleid = "tupleid"
  private val tablename = "tablename"
  private val attribute = "attribute"
  private val oldvalue = "oldvalue"
  private val newvalue = "newvalue"
  private val time = "time"

  private val schema: Seq[String] = Seq(id, vid, tupleid, tablename, attribute, oldvalue, newvalue, time)

  def getSchema: Seq[String] = schema

}
