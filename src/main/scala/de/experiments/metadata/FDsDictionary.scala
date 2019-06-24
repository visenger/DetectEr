package de.experiments.metadata

trait FDsDictionary extends Serializable {
  val allFDs: List[FD]
}

object HospFDsDictionary extends FDsDictionary {

  val zip = "zip"
  val city = "city"
  val phone = "phone"
  val address = "address"
  val state = "state"
  val prno = "prno"
  val mc = "mc"
  val stateavg = "stateavg"

  /** ALL FDs for the hosp data
    * zip -> city
    * zip -> state
    * zip, address -> phone
    * city, address -> phone
    * state, address -> phone
    * prno, mc -> stateavg
    * */

  val fd1 = FD(List(zip), List(city))
  val fd12 = FD(List(zip), List(state))
  val fd2 = FD(List(zip, address), List(phone))
  val fd3 = FD(List(city, address), List(phone))
  val fd4 = FD(List(state, address), List(phone))
  val fd5 = FD(List(prno, mc), List(stateavg))

  val allFDs: List[FD] = List(fd1,fd12, fd2, fd3, fd4, fd5)


}

object FlightsFDsDictionary extends FDsDictionary {
  /**
    *
    * Flight->ActualDeparture
    * Flight->DepartureGate
    * Flight->ArrivalGate
    * Flight->ScheduledArrival
    * Flight->ActualArrival
    * Flight->ScheduledDeparture
    *
    * ActualArrival,ActualDeparture,ScheduledArrival->ScheduledDeparture
    * ActualArrival,ActualDeparture,ScheduledArrival->DepartureGate
    * ActualArrival,ActualDeparture,ScheduledArrival->Flight
    * ActualDeparture,ScheduledArrival,ScheduledDeparture->DepartureGate
    * ActualDeparture,ScheduledArrival,ScheduledDeparture->Flight
    * ActualDeparture,DepartureGate,ScheduledArrival->ScheduledDeparture
    * ActualDeparture,DepartureGate,ScheduledArrival->Flight
    * ActualDeparture,ArrivalGate,ScheduledArrival->ScheduledDeparture
    * ActualDeparture,ArrivalGate,ScheduledArrival->Flight
    * ActualDeparture,Flight,ScheduledArrival->DepartureGate
    * ActualArrival,ScheduledArrival,ScheduledDeparture->DepartureGate
    * ActualArrival,ScheduledArrival,ScheduledDeparture->Flight
    * ActualArrival,DepartureGate,ScheduledArrival->ScheduledDeparture
    * ActualArrival,DepartureGate,ScheduledArrival->Flight
    * ActualArrival,ArrivalGate,ScheduledArrival->ScheduledDeparture
    * ActualArrival,ArrivalGate,ScheduledArrival->Flight
    * ActualArrival,Flight,ScheduledArrival->DepartureGate
    * ArrivalGate,ScheduledArrival->DepartureGate
    * Flight,ScheduledArrival->ScheduledDeparture
    **/

  val recid = "RowId"
  val source = "Source"
  val flight = "Flight"
  val scheduleddeparture = "ScheduledDeparture"
  val actualdeparture = "ActualDeparture"
  val departuregate = "DepartureGate"
  val scheduledarrival = "ScheduledArrival"
  val actualarrival = "ActualArrival"
  val arrivalgate = "ArrivalGate"

  val fd1 = FD(List(flight), List(actualdeparture))
  val fd2 = FD(List(flight), List(actualarrival))
  val fd3 = FD(List(flight), List(departuregate))
  val fd4 = FD(List(flight), List(arrivalgate))

  val allFDs: List[FD] = List(fd1, fd2, fd3, fd4)

}

object BeersFDsDictionary extends FDsDictionary {

  /**
    * brewery id -> brewery name,
    * brewery id -> city,
    * brewery id -> state,
    * city -> state
    **/
  private val recId = "tid"
  private val id = "id"
  private val beer_name = "beer-name"
  private val style = "style"
  private val ounces = "ounces"
  private val abv = "abv"
  private val ibu = "ibu"
  private val brewery_id = "brewery_id"
  private val brewery_name = "brewery-name"
  val city = "city"
  val state = "state"

  val fd1 = FD(List(brewery_id), List(brewery_name))
  val fd2 = FD(List(brewery_id), List(city))
  val fd3 = FD(List(brewery_id), List(state))
  val fd4 = FD(List(city), List(state))

  override val allFDs: List[FD] = List(fd1, fd2, fd3, fd4)

}

object MuseumFDsDictionary extends FDsDictionary {

  val objectNumber = "Object Number"
  val isHighlight = "Is Highlight"
  val isPublicDomain = "Is Public Domain"
  val objectID = "Object ID"
  val department = "Department"
  val objectName = "Object Name"
  val title = "Title"
  val culture = "Culture"
  val period = "Period"
  val dynasty = "Dynasty"
  val reign = "Reign"
  val portfolio = "Portfolio"
  val artistRole = "Artist Role"
  val artistPrefix = "Artist Prefix"
  val artistDisplayName = "Artist Display Name"
  val artistDisplayBio = "Artist Display Bio"
  val artistSuffix = "Artist Suffix"
  val artistAlphaSort = "Artist Alpha Sort"
  val artistNationality = "Artist Nationality"
  val artistBeginDate = "Artist Begin Date"
  val artistEndDate = "Artist End Date"
  val objectDate = "Object Date"
  val objectBeginDate = "Object Begin Date"
  val objectEndDate = "Object End Date"
  val medium = "Medium"
  val dimensions = "Dimensions"
  val creditLine = "Credit Line"
  val geographyType = "Geography Type"
  val city = "City"
  val state = "State"
  val county = "County"
  val country = "Country"
  val region = "Region"
  val subregion = "Subregion"
  val locale = "Locale"
  val locus = "Locus"
  val excavation = "Excavation"
  val river = "River"
  val classification = "Classification"
  val rightsAndReproduction = "Rights and Reproduction"
  val linkResource = "Link Resource"
  val metadataDate = "Metadata Date"
  val repository = "Repository"

  val fd1 = FD(List(city), List(country))
  val fd2 = FD(List(artistDisplayName), List(artistBeginDate))
  val fd3 = FD(List(artistDisplayName), List(artistEndDate))

  override val allFDs: List[FD] = List(fd1, fd2, fd3)
}

object BlackOakFDsDictionary extends FDsDictionary {

  /**
    *
    * RecID->Address
    * RecID->SSN
    * RecID->LastName
    * RecID->DOB
    * RecID->POBox
    * RecID->City
    * RecID->FirstName
    * RecID->POCityStateZip
    * RecID->ZIP
    * RecID->MiddleName
    * RecID->State
    * Address->State
    * ZIP->State
    * Address,LastName,POBox,SSN->POCityStateZip
    * Address,LastName,MiddleName,SSN->POCityStateZip
    * Address,FirstName,LastName,SSN->POBox
    * Address,LastName,POCityStateZip,SSN->POBox
    * Address,LastName,MiddleName,SSN->POBox
    * Address,City,DOB,POBox,SSN->POCityStateZip
    * Address,MiddleName,POBox,SSN->POCityStateZip
    * Address,FirstName,POCityStateZip,SSN->POBox
    * Address,FirstName,MiddleName,SSN->POBox
    * Address,FirstName,MiddleName,SSN->POCityStateZip
    * Address,DOB,LastName,POBox->POCityStateZip
    * Address,City,DOB,LastName,MiddleName,POCityStateZip->POBox
    * Address,DOB,FirstName,LastName->POBox
    * Address,LastName,MiddleName,POBox->POCityStateZip
    * Address,City,FirstName,LastName->POBox
    * Address,FirstName,LastName->POCityStateZip
    * Address,DOB,FirstName,POCityStateZip->POBox
    * Address,DOB,FirstName,MiddleName->POBox
    * Address,DOB,FirstName,MiddleName->POCityStateZip
    * Address,FirstName,POBox->POCityStateZip
    * Address,City,FirstName,POCityStateZip->POBox
    * City,DOB,FirstName,LastName,POCityStateZip,SSN->POBox
    * DOB,FirstName,LastName,POCityStateZip,SSN,ZIP->POBox
    * DOB,LastName,MiddleName,POCityStateZip,SSN,ZIP->POBox
    * FirstName,LastName,POBox,SSN,ZIP->POCityStateZip
    * DOB,FirstName,MiddleName,POBox,SSN,ZIP->POCityStateZip
    * DOB,FirstName,LastName,POBox,ZIP->POCityStateZip
    *
    **/


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

  val fd1 = FD(List(firstname, lastname), List(address))
  override val allFDs = List(fd1)
}

object SalariesFDsDictionary extends FDsDictionary {
  private val id = "id"
  private val employeename = "employeename"
  private val jobtitle = "jobtitle"
  private val basepay = "basepay"
  private val overtimepay = "overtimepay"
  private val otherpay = "otherpay"
  private val benefits = "benefits"
  private val totalpay = "totalpay"
  private val totalpaybenefits = "totalpaybenefits"
  private val year = "year"
  private val notes = "notes"
  private val agency = "agency"
  private val status = "status"

  val fd = FD(List(jobtitle), List(basepay))
  override val allFDs = List(fd)
}
