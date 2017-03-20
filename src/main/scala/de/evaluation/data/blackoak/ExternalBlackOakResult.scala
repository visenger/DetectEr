package de.evaluation.data.blackoak

import com.typesafe.config.ConfigFactory
import de.evaluation.data.gold.standard.GoldStandardCreator
import de.evaluation.data.schema.BlackOakSchema
import de.evaluation.data.util.FullResultCreator
import de.evaluation.f1.{Cells, GoldStandard}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 16/03/17.
  */
class ExternalBlackOakResult {

}

object ExternalBlackOakResultRunner {

  val cleanData = "data.BlackOak.clean-data-path"
  val dirtyData = "data.BlackOak.dirty-data-path"


  def main(args: Array[String]): Unit = {
    val creator = GoldStandardCreator
    val s = BlackOakSchema
    creator.onSchema(s)
    creator.addDirtyPath(dirtyData)
    creator.addCleanPath(cleanData)
    creator.specifyOutputFolder("output.blackoak.external.goldstandard.ground.truth.folder")
    creator.external_GoldStandard()
  }
}

/**
  *
  * tools {
  * dboost = ${home.dir.external.tools}"/dBoost.csv"
  * gaussian = ${home.dir.external.tools}"/gaussian.csv"
  * google.refine = ${home.dir.external.tools}"/GRefine_detectedCells.csv"
  * histograms = ${home.dir.external.tools}"/histograms.csv"
  * katara = ${home.dir.external.tools}"/katara.csv"
  * mixture = ${home.dir.external.tools}"/mixture.csv"
  * rulebased = ${home.dir.external.tools}"/rulebased.csv"
  * tamr = ${home.dir.external.tools}"/tamr.csv"
  * trifacta = ${home.dir.external.tools}"/Trifacta_detectedCells.csv"
  * *
  * }
  **/

object ExternalBlackOakFullResultRunner {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("external.conf")

    val groundTruthPath = config.getString("output.blackoak.external.goldstandard.ground.truth.file")

    //val dboostPath = config.getString("tools.dboost")
    val gaussianPath = config.getString("tools.gaussian")
    val googleRefinePath = config.getString("tools.google.refine")
    val histogramsPath = config.getString("tools.histograms")
    val kataraPath = config.getString("tools.katara")
    val mixturePath = config.getString("tools.mixture")
    val rulebasedPath = config.getString("tools.rulebased")
    val tamrPath = config.getString("tools.tamr")
    val trifactaPath = config.getString("tools.trifacta")


    SparkLOAN.withSparkSession("EXT-FULL-RESULT") {
      session => {
        val groundTruth = DataSetCreator.createFrameNoHeader(session, groundTruthPath, GoldStandard.schema: _*)

        //val dboostResult = DataSetCreator.createFrameNoHeader(session, dboostPath, Cells.schema: _*)
        val gaussianResult = DataSetCreator.createFrameNoHeader(session, gaussianPath, Cells.schema: _*)
        val googleRefineResult = DataSetCreator.createFrameNoHeader(session, googleRefinePath, Cells.schema: _*)
        val histogramResult = DataSetCreator.createFrameNoHeader(session, histogramsPath, Cells.schema: _*)
        val kataraResult = DataSetCreator.createFrameNoHeader(session, kataraPath, Cells.schema: _*)
        val mixtureResult = DataSetCreator.createFrameNoHeader(session, mixturePath, Cells.schema: _*)
        val rulebasedResult = DataSetCreator.createFrameNoHeader(session, rulebasedPath, Cells.schema: _*)
        val tamrResult = DataSetCreator.createFrameNoHeader(session, tamrPath, Cells.schema: _*)
        val trifactaResult = DataSetCreator.createFrameNoHeader(session, trifactaPath, Cells.schema: _*)

        val allResults: Seq[DataFrame] =
          Seq(gaussianResult,
            googleRefineResult,
            histogramResult,
            kataraResult,
            mixtureResult,
            rulebasedResult,
            tamrResult,
            trifactaResult)

        val fullResultCreator = new FullResultCreator(session)
        fullResultCreator.onToolsResults(allResults)
        fullResultCreator.onGroundTruth(groundTruth)
        val fullResult: DataFrame = fullResultCreator.getFullResult
        fullResult.show()

        val outputPath = config.getString("output.blackoak.external.full.result.folder")

        fullResult
          .coalesce(1)
          .write
          .format("com.databricks.spark.csv")
          .option("header", true)
          .save(outputPath)


      }
    }

  }
}
