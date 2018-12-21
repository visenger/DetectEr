package de.playground

import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.util.SparkLOAN
import de.experiments.ExperimentsCommonConfig
import de.model.util.NumbersUtil
import de.util.DatasetFlattener
import org.apache.spark.sql.DataFrame

object MetadataPlayground {
  //val metadataPath = "/Users/visenger/deepdive_notebooks/hosp-cleaning/dirty-data/SCDP-1.1-SNAPSHOT.jar2018-04-27T143644_stats"
  //val metadataPath = "/Users/visenger/research/datasets/craft-beers/craft-cans/metadata/SCDP-1.1-SNAPSHOT.jar2018-05-02T110514_stats"
  //val metadataPath = "/Users/visenger/research/datasets/craft-beers/craft-cans/metadata/dirtySCDP-1.1-SNAPSHOT.jar2018-05-18T125830_stats"
  val metadataPath = "/Users/visenger/research/datasets/craft-beers/craft-cans/metadata/dirty-2-SCDP-1.1-SNAPSHOT.jar2018-05-22T150721_stats"

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("metadata reader") {
      session => {
        val creator = MetadataCreator.apply()

        val fullMetadataDF: DataFrame = creator.getFullMetadata(session, metadataPath)
        fullMetadataDF.printSchema()

        /**
          * root
          * |-- nulls count: long (nullable = true)
          * |-- % of nulls: long (nullable = true)
          * |-- % of distinct vals: long (nullable = true)
          * |-- top10: array (nullable = true)
          * |    |-- element: string (containsNull = true)
          * |-- freqTop10: array (nullable = true)
          * |    |-- element: long (containsNull = true)
          * |-- histogram: array (nullable = true)
          * |    |-- element: string (containsNull = true)
          * |-- attrName: string (nullable = true)
          */
        fullMetadataDF.show(false)

      }
    }
  }
}

object AddressPlayground extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("plgrd") {
      session => {
        val datasets = Seq("museum", "beers")
        datasets.foreach(dataset => {
          println(s"playground data: $dataset.....")

          val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
          val fullMetadataDF: DataFrame = creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
          fullMetadataDF.show(50)

          val flatWithLabelDF: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)
          //flatWithLabelDF.show()

          val totalCount: Long = flatWithLabelDF.count()

          val errorsCount: Long = flatWithLabelDF.where(flatWithLabelDF("label") === "1").count()

          val errPercentage: Double = errorsCount * 100 / totalCount.toDouble

          println(s" dataset: $dataset -> error rate: ${NumbersUtil.round(errPercentage)}")

          //        val flatWithMetadataDF: DataFrame = flatWithLabelDF.join(fullMetadataDF, Seq("attrName"))
          //
          //flatWithMetadataDF.printSchema()
        })
      }
    }
  }
}
