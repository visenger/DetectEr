package de.evaluation.data.hosp

import de.evaluation.data.gold.standard.GoldStandardCreator
import de.evaluation.data.schema.HospSchema

/**
  * Created by visenger on 09/02/17.
  */
class HospGoldStandard {
  /*
  *
  *
  data.hosp.clean.1k
  data.hosp.dirty.1k
  data.hosp.gold.1k

  data.hosp.clean.10k
  data.hosp.dirty.10k
  data.hosp.gold.10k

  data.hosp.clean.100k
  data.hosp.dirty.100k
  data.hosp.gold.100k
  * */

  val cleanData = "data.hosp.clean.10k"
  val dirtyData = "data.hosp.dirty.10k"
  val outputFolder = "data.hosp.gold.10k"

  def createGoldStd(): Unit = {
    val s = HospSchema
    val creator = GoldStandardCreator
    creator.onSchema(s)
    creator.addDirtyPath(dirtyData)
    creator.addCleanPath(cleanData)
    creator.specifyOutputFolder(outputFolder)
    creator.dirtySolution(43)

  }


}

object HospGoldStandardRunner {
  def main(args: Array[String]): Unit = {
    new HospGoldStandard().createGoldStd()
  }

}
