package de.evaluation.data.schema

/**
  * HOSP dataset schema;
  */
object HospSchema extends Schema {


  private val recid = "oid"

  private val schema = Seq(recid, "prno", "hospitalname", "address", "city", "state", "zip", "countryname", "phone", "hospitaltype", "hospitalowner", "emergencyservice", "condition", "mc", "measurename", "score", "sample", "stateavg")

  override def getSchema: Seq[String] = schema

  override def getRecID: String = recid
}
