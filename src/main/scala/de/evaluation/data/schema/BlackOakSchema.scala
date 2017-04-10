package de.evaluation.data.schema

/**
  * Created by visenger on 27/11/16.
  */

object BlackOakSchema extends Schema with AttributesDataType {


  private val recid = "RecID"
  private val firstname = "FirstName"
  private val middlename = "MiddleName"
  private val lastname = "LastName"
  private val address = "Address"
  private val city = "City"
  private val stateAttr = "State"
  private val zipAttr = "ZIP"
  private val pobox = "POBox"
  private val pocitystatezip = "POCityStateZip"
  private val ssnAttr = "SSN"
  private val dobAttr = "DOB"

  val schema = Seq(recid, firstname, middlename,
    lastname, address, city, stateAttr, zipAttr, pobox, pocitystatezip, ssnAttr, dobAttr)

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

  override def dataTypesDictionary: Map[String, String] = Map(
    recid -> string,
    firstname -> string,
    middlename -> string,
    lastname -> string,
    address -> address,
    city -> string,
    stateAttr -> string,
    zipAttr -> zipType,
    pobox -> string,
    pocitystatezip -> address,
    ssnAttr -> ssn,
    dobAttr -> date
  )
}
