package de.evaluation.data.schema

/**
  * HOSP dataset schema;
  */
object HospSchema extends Schema with AttributesDataType {


  val recid = "oid"

  val prno = "prno"
  val hospitalname = "hospitalname"
  val address = "address"
  val city = "city"
  val state = "state"
  val zip = "zip"
  val countryname = "countryname"
  val phone = "phone"
  val hospitaltype = "hospitaltype"
  val hospitalowner = "hospitalowner"
  val emergencyservice = "emergencyservice"
  val condition = "condition"
  val mc = "mc"
  val measurename = "measurename"
  val score = "score"
  val sample = "sample"
  val stateavg = "stateavg"

  private val schema = Seq(recid, prno,
    hospitalname, address,
    city, state, zip,
    countryname, phone,
    hospitaltype, hospitalowner,
    emergencyservice, condition,
    mc, measurename, score, sample, stateavg)

  override def getSchema: Seq[String] = schema

  override def getRecID: String = recid

  override def dataTypesDictionary = Map(
    prno -> zipType,
    hospitalname -> string,
    address -> string,
    city -> string,
    state -> string,
    zip -> zipType,
    countryname -> string,
    phone -> phoneNumType,
    hospitaltype -> string,
    hospitalowner -> string,
    emergencyservice -> boolean,
    condition -> string,
    mc -> string,
    measurename -> string,
    score -> string,
    sample -> string,
    stateavg -> string
  )
}
