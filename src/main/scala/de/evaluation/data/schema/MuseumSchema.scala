package de.evaluation.data.schema

object MuseumSchema extends Schema with AttributesDataType {

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

  override def getSchema: Seq[String] = Seq(objectNumber,
    isHighlight,
    isPublicDomain,
    objectID,
    department,
    objectName,
    title,
    culture,
    period,
    dynasty,
    reign,
    portfolio,
    artistRole,
    artistPrefix,
    artistDisplayName,
    artistDisplayBio,
    artistSuffix,
    artistAlphaSort,
    artistNationality,
    artistBeginDate,
    artistEndDate,
    objectDate,
    objectBeginDate,
    objectEndDate,
    medium,
    dimensions,
    creditLine,
    geographyType,
    city,
    state,
    county,
    country,
    region,
    subregion,
    locale,
    locus,
    excavation,
    river,
    classification,
    rightsAndReproduction,
    linkResource,
    metadataDate,
    repository)

  override def getRecID: String = objectID

  override def dataTypesDictionary: Map[String, String] = Map(
    objectNumber -> string,
    isHighlight -> boolean,
    isPublicDomain -> boolean,
    objectID -> string,
    department -> string,
    objectName -> string,
    title -> string,
    culture -> string,
    period -> string,
    dynasty -> string,
    reign -> string,
    portfolio -> string,
    artistRole -> string,
    artistPrefix -> string,
    artistDisplayName -> string,
    artistDisplayBio -> string,
    artistSuffix -> string,
    artistAlphaSort -> string,
    artistNationality -> string,
    artistBeginDate -> integer,
    artistEndDate -> integer,
    objectDate -> string,
    objectBeginDate -> integer,
    objectEndDate -> integer,
    medium -> string,
    dimensions -> string,
    creditLine -> string,
    geographyType -> string,
    city -> string,
    state -> string,
    county -> string,
    country -> string,
    region -> string,
    subregion -> string,
    locale -> string,
    locus -> string,
    excavation -> string,
    river -> string,
    classification -> string,
    rightsAndReproduction -> string,
    linkResource -> urlType,
    metadataDate -> date,
    repository -> string
  )


}
