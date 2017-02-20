package de.evaluation.data.salaries

import de.evaluation.data.gold.standard.GoldStandardCreator
import de.evaluation.data.schema.SalariesSchema
import de.evaluation.f1.GoldStandard
import de.evaluation.util.DataSetCreator
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by visenger on 10/02/17.
  */
class SalariesGoldStandard {

  val cleanData = "data.salaries.clean"
  val dirtyData = "data.salaries.dirty"
  val outputFolder = "data.salaries.gold"

  def createGoldStd(): Unit = {
    val s = SalariesSchema
    val creator = GoldStandardCreator
    creator.onSchema(s)
    creator.addCleanPath(cleanData)
    creator.addDirtyPath(dirtyData)
    creator.specifyOutputFolder(outputFolder)
    creator.dirtySolution(13)
  }


}

object SalariesGoldStandardRunner {

  def main(args: Array[String]): Unit = {
    new SalariesGoldStandard().createGoldStd()

  }

  def getGroundTruth(session: SparkSession): DataFrame = {
    val file = "result.salaries.gold"
    val groundTruth: DataFrame = DataSetCreator.createDataSetNoHeader(session, file, GoldStandard.schema: _*)
    groundTruth
  }

}
