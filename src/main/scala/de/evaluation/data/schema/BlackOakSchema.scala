package de.evaluation.data.schema

import scala.collection.generic.GenericCompanion

/**
  * Created by visenger on 27/11/16.
  */

object BlackOakSchema extends Schema {


  private val recid = "RecID"
  val schema = Seq(recid, "FirstName", "MiddleName", "LastName", "Address", "City", "State", "ZIP", "POBox", "POCityStateZip", "SSN", "DOB")

  val indexedAttributes: Map[String, Int] = indexAttributes

  val indexedLowerCaseAttributes: Map[String, Int] = indexLCAttributes
  //  private def indexAttributes = {
  //    schema.zipWithIndex.toMap.map(
  //      e => (e._1, {
  //        var i = e._2;
  //        i += 1; // 1-based indexing
  //        i
  //      }))
  //  }

  //  private def indexLCAttributes: Map[String, Int] = {
  //    indexedAttributes.map(a => (a._1.toLowerCase, a._2))
  //  }

  //  def getIndexesByAttrNames(attributes: List[String]): List[Int] = {
  //    val allAttrToLowerCase = attributes.map(_.toLowerCase)
  //    val indexes = allAttrToLowerCase.map(attr => {
  //      indexedLowerCaseAttributes.getOrElse(attr, 0)
  //    })
  //    indexes.sortWith(_ < _)
  //  }

  override def getSchema(): Seq[String] = schema

  override def getRecID: String = recid
}
