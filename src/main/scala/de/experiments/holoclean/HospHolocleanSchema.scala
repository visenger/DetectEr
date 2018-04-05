package de.experiments.holoclean

object HospHolocleanSchema {
  var index = "index"
  var ProviderNumber = "ProviderNumber"
  var HospitalName = "HospitalName"
  var Address1 = "Address1"
  var Address2 = "Address2"
  var Address3 = "Address3"
  var City = "City"
  var State = "State"
  var ZipCode = "ZipCode"
  var CountyName = "CountyName"
  var PhoneNumber = "PhoneNumber"
  var HospitalType = "HospitalType"
  var HospitalOwner = "HospitalOwner"
  var EmergencyService = "EmergencyService"
  var Condition = "Condition"
  var MeasureCode = "MeasureCode"
  var MeasureName = "MeasureName"
  var Score = "Score"
  var Sample = "Sample"
  var Stateavg = "Stateavg"

  val schema = Seq(index,
    ProviderNumber,
    HospitalName,
    Address1,
    Address2,
    Address3,
    City,
    State,
    ZipCode,
    CountyName,
    PhoneNumber,
    HospitalType,
    HospitalOwner,
    EmergencyService,
    Condition,
    MeasureCode,
    MeasureName,
    Score,
    Sample,
    Stateavg)

  val attrToIdx = Map(index -> 0,
    ProviderNumber -> 1,
    HospitalName -> 2,
    Address1 -> 3,
    Address2 -> 4,
    Address3 -> 5,
    City -> 6,
    State -> 7,
    ZipCode -> 8,
    CountyName -> 9,
    PhoneNumber -> 10,
    HospitalType -> 11,
    HospitalOwner -> 12,
    EmergencyService -> 13,
    Condition -> 14,
    MeasureCode -> 15,
    MeasureName -> 16,
    Score -> 17,
    Sample -> 18,
    Stateavg -> 19)

  val idxToAttr: Map[Int, String] = attrToIdx.map(_.swap)


  /**
    * Our mapping: (holoclean mapping)
    * 5 -> city (6)
    * 12 -> emergencyservice(13)
    * 7 -> zip(8)
    * 6 -> state (7)
    * 3 -> hospitalname (2)
    * 1 -> oid (0)
    * 16 -> score(17)
    * 11 -> hospitalowner()
    * 18 -> stateavg
    * 13 -> condition
    * 8 -> countryname
    * 4 -> address
    * 17 -> sample
    * 2 -> prno
    * 14 -> mc
    * 10 -> hospitaltype
    * 15 -> measurename
    * 9 -> phone
    *
    */

}
