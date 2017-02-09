package de.evaluation.data.schema

/**
  * Created by visenger on 09/02/17.
  */
trait Schema {

  def getIndexesByAttrNames(attributes: List[String]): List[Int]

  def getSchema: Seq[String]

  def getRecID: String

}

