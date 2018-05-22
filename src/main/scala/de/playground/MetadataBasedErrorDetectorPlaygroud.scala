package de.playground

import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.util.SparkLOAN
import de.experiments.ExperimentsCommonConfig
import de.util.DatasetFlattener
import org.apache.spark.sql.DataFrame

/**
  * prototype to an error detection based on metadata information;
  */
object MetadataBasedErrorDetectorPlaygroud extends ExperimentsCommonConfig {


  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("metadata reader") {
      session => {

        Seq("flights", "beers").foreach(dataset => {
          println(s"processing $dataset.....")
          val metadataPath = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val fullMetadataDF: DataFrame = creator.getFullMetadata(session, metadataPath)
          //          fullMetadataDF.printSchema()

          fullMetadataDF.show(false)

          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
          dirtyDF.show()
          DatasetFlattener().onDataset(dataset).flattenCleanData(session).show()
          DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session).show()

          dirtyDF.join(fullMetadataDF, Seq("attrName")).show()


        })


      }
    }
  }
}
