package de.evaluation.data.schema

/**
  * default (dummy) schema;
  */
object DefaultSchema extends Schema with AttributesDataType {
  private val recid = "recid"

  override def getSchema: Seq[String] = Seq(recid)

  override def getRecID: String = recid

  override def dataTypesDictionary: Map[String, String] = Map(recid -> integer)
}
