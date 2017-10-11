package de.experiments.features.descriptive.statistics

import de.evaluation.f1.FullResult
import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.DataFrame

object FeaturesDescriptiveStatisticsRunner {


  def main(args: Array[String]): Unit = {


    val datasets = Seq("blackoak", "hosp", "salaries", "flights")

    datasets.foreach(dataset => {
      val allMetadata: Seq[String] = createDescriptiveStatisticsFor(dataset)
    })

  }


  def createDescriptiveStatisticsFor(dataset: String): Seq[String] = {

    var result: Seq[String] = List()
    SparkLOAN.withSparkSession("DescriptiveStatsForFeatures") {
      session => {
        val featuresStats = FeaturesDescriptiveStatistics.init
          .onDatasetName(dataset)
          .get()

        val metadataDF: DataFrame = featuresStats.createAllMetadataForTrain(session)

        metadataDF.printSchema()

        val allMetadataCols = featuresStats.getAllMetadataFeatures()
        allMetadataCols.foreach(metadata => println(metadata))
        val schema = featuresStats.getSchema
        val allAttributesNames = schema.getSchema.filterNot(_.equalsIgnoreCase(FullResult.recid))

        val metaForAllAttrs: Seq[String] = allAttributesNames.flatMap(attr => {
          println(s"DF for the attribute: $attr")
          //todo: the attribute specific df can be empty.
          val attrSpecificDF: DataFrame = featuresStats.createElementsListForAttribute(metadataDF, attr)
          val totalCount = attrSpecificDF.count()

          val allMetadataInfoForAttr: Seq[String] = for {
            metaCol <- allMetadataCols
            if (totalCount > 0)
          } yield {
            val metaColumnCount = attrSpecificDF.where(attrSpecificDF(metaCol) === 1.0).count()

            val dirtyPercentage = metaColumnCount.toDouble * 100 / totalCount.toDouble

            s"$attr: meta column: $metaCol dirty percentage: $dirtyPercentage %"

          }
          allMetadataInfoForAttr
        }
        )
        result = result ++ metaForAllAttrs

      }

    }
    result
  }
}
