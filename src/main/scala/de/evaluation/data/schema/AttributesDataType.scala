package de.evaluation.data.schema

/**
  * Created by visenger on 07/04/17.
  */
trait AttributesDataType {

  val string = "String"
  val integer = "Integer"
  val decimal = "Decimal"
  val boolean = "Boolean"
  val map = "Map"
  val array = "Array"
  val ipAddress = "IP Address"
  val urlType = "URL"
  val httpCode = "HTTP Code"
  val date = "Date/Time"
  val ssn = "Social Security Number"
  val phoneNumType = "Phone Number"
  val email = "Email Address"
  val creditCard = "Credit Card"
  val gender = "Gender"
  val zipType = "Zip Code"
  val text = "Text"

  val dataTypes = Seq(string, integer, decimal, boolean, map, array, ipAddress,
    urlType, httpCode, date, ssn, phoneNumType, email, creditCard, gender, zipType)


}
