package de.playground

import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.DataFrame

/**
  * prototype to an error detection based on metadata information;
  */
class MetadataBasedErrorDetectorPlaygroud {

  val metadataPath = "/Users/visenger/research/datasets/craft-beers/craft-cans/metadata/dirty-2-SCDP-1.1-SNAPSHOT.jar2018-05-22T150721_stats"

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("metadata reader") {
      session => {
        val creator = MetadataCreator()

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
