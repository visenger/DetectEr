package de.experiments.features.generation

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{Eval, FullResult}
import de.evaluation.util.SparkLOAN
import de.experiments.models.combinator.Bagging
import org.apache.spark.sql.DataFrame

object FeaturesGeneratorRunner {

  def main(args: Array[String]): Unit = {
    val experimentsConf = ConfigFactory.load("experiments.conf")
    val datasets = Seq("blackoak", "hosp", "salaries", "flights")

    datasets.foreach(dataset => {
      (2 to 4).foreach(i => {
        // val systems: Seq[String] = experimentsConf.getStringList(s"$dataset.k.$i").asScala.toSeq

        // println(s"running on ${dataset.toUpperCase()} | clusters number k=$i | on systems: ${systems.mkString(", ")}")
        println(s"running on ${dataset.toUpperCase()} ")
        singleRun(dataset)
      })
    })

  }

  def singleRun(dataset: String, tools: Seq[String] = FullResult.tools): Unit = {

    SparkLOAN.withSparkSession("METADATA-COMBI") {
      session => {
        // val allFDs = fdsDictionary.allFDs
        val generator = FeaturesGenerator.init


        val dirtyDF: DataFrame = generator
          .onDatasetName(dataset)
          .onTools(tools)
          .getDirtyData(session)
          .cache()

        //Set of content-based metadata, such as "attrName", "attrType", "isNull", "missingValue", "attrTypeIndex", "attrTypeVector", "isTop10"
        val contentBasedFeaturesDF: DataFrame = generator.generateContentBasedMetadata(session, dirtyDF)

        //FD-partiotion - based features
        val fdMetadataDF: DataFrame = generator.generateFDMetadata(session, dirtyDF, generator.allFDs)

        //general info about fd-awarnes of each cell
        val generalInfoDF = generator.generateGeneralInfoMetadata(session, dirtyDF, generator.allFDs)

        //all features data frames should contain Seq(Cells.recid, Cells.attrnr, "value") attributes in order to join with other DFs
        val allMetadataDF = generator.accumulateAllFeatures(session, Seq(contentBasedFeaturesDF, fdMetadataDF, generalInfoDF))

        //Systems features contains the encoding of each system result and the total numer of systems identified the particular cell as an error.
        val (testDF: DataFrame, trainDF: DataFrame) = generator.createSystemsFeatures(session)

        val (fullTrainDF: DataFrame, fullTestDF: DataFrame) = generator.accumulateDataAndMetadata(session, trainDF, testDF, allMetadataDF)

        //Run combinations.
        //        val stacking = new Stacking()
        //        val evalStacking: Eval = stacking.performStackingOnToolsAndMetadata(session, fullTrainDF, fullTestDF)
        //        evalStacking.printResult(s"STACKING on $dataset")

        val bagging = new Bagging()
        val evalBagging: Eval = bagging.performBaggingOnToolsAndMetadata(session, fullTrainDF, fullTestDF)
        evalBagging.printResult(s"BAGGING on $dataset")

      }
    }


  }
}
