package de.evaluation.data.schema

object BeersSchema extends Schema with AttributesDataType {


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
  val beersSchema = Seq(recId, id, beer_name, style, ounces, abv, ibu,
    brewery_id, brewery_name, city, state)

  override def getSchema: Seq[String] = beersSchema

  override def getRecID: String = recId

  override def dataTypesDictionary: Map[String, String] = Map(
    recId -> integer,
    id -> integer,
    beer_name -> string,
    style -> string,
    ounces -> integer,
    abv -> integer,
    ibu -> integer,
    brewery_id -> string,
    brewery_name -> string,
    city -> string,
    state -> stateType
  )
}
