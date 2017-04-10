package de.evaluation.data.flights

import de.evaluation.data.gold.standard.GoldStandardCreator
import de.evaluation.data.schema.FlightsSchema

/**
  * Created by visenger on 06/04/17.
  */
object FlightsGoldStandard {

  val cleanDataConf="data.flights.clean"
  val dirtyDataConf="data.flights.dirty"
  val goldStandardOutput="data.flights.gold"

  def main(args: Array[String]): Unit = {
    val creator=GoldStandardCreator
    val schema=FlightsSchema
    creator.addCleanPath(cleanDataConf)
    creator.addDirtyPath(dirtyDataConf)
    creator.onSchema(schema)
    creator.specifyOutputFolder(goldStandardOutput)
    creator.create

  }

}
