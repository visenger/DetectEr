package de.evaluation.data.schema

/**
  * HOSP dataset schema;
  */
object HospSchema extends Schema with AttributesDataType {


  private val recid = "oid"

  private val prno = "prno"
  private val hospitalname = "hospitalname"
  private val address = "address"
  private val city = "city"
  private val state = "state"
  private val zip = "zip"
  private val countryname = "countryname"
  private val phone = "phone"
  private val hospitaltype = "hospitaltype"
  private val hospitalowner = "hospitalowner"
  private val emergencyservice = "emergencyservice"
  private val condition = "condition"
  private val mc = "mc"
  private val measurename = "measurename"
  private val score = "score"
  private val sample = "sample"
  private val stateavg = "stateavg"

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
