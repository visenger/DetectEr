package de.evaluation.data.schema

/**
  * Created by visenger on 06/04/17.
  */
object FlightsSchema extends Schema with AttributesDataType {
  private val recid = "RowId"
  private val source = "Source"
  private val flight = "Flight"
  private val scheduleddeparture = "ScheduledDeparture"
  private val actualdeparture = "ActualDeparture"
  private val departuregate = "DepartureGate"
  private val scheduledarrival = "ScheduledArrival"
  private val actualarrival = "ActualArrival"
  private val arrivalgate = "ArrivalGate"
  private val schema = Seq(recid, source, flight,
    scheduleddeparture, actualdeparture,
    departuregate, scheduledarrival,
    actualarrival, arrivalgate)

  override def getSchema: Seq[String] = schema

  override def getRecID: String = recid

  override def dataTypesDictionary: Map[String, String] = Map(
    recid -> string,
    source -> string,
    flight -> string,
    scheduleddeparture -> date,
    actualdeparture -> date,
    departuregate -> string,
    scheduledarrival -> date,
    actualarrival -> date,
    arrivalgate -> string
  )
}
