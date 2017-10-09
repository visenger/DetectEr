package de.experiments.features.descriptive.statistics

import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.DataFrame

object FeaturesDescriptiveStatisticsRunner {


  def main(args: Array[String]): Unit = {


    val datasets = Seq("blackoak" /*, "hosp", "salaries", "flights"*/)

    datasets.foreach(dataset => {
      createDiscriptiveStatisticsFor(dataset)
    })

  }


  def createDiscriptiveStatisticsFor(dataset: String) = {
    SparkLOAN.withSparkSession("DescriptiveStatsForFeatures") {
      session => {
        val featuresStats: FeaturesDescriptiveStatistics = FeaturesDescriptiveStatistics.init
          .onDatasetName(dataset)
          .get()

        val metadataDF: DataFrame = featuresStats.createAllMetadataForTrain(session)

        metadataDF.printSchema()

        metadataDF.show(45, false)

        featuresStats.getAllMetadataFeatures().foreach(metadata => println(metadata))


      }
    }
  }

}
