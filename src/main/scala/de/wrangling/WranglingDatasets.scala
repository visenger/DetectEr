package de.wrangling

import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.HospSchema
import de.evaluation.f1.{Cells, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by visenger on 07/04/17.
  */
class WranglingDatasets extends ExperimentsCommonConfig {

  private var datasetName = ""


  def onDatasetName(name: String): this.type = {
    datasetName = name
    this
  }

  private def getDirtyDataPath: String = allRawData.getOrElse(datasetName, "unknown")

  //  def createMetadataFeatures(): DataFrame = {
  //
  //  }

}

object WranglingHosp extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    val datasetName = "blackoak"
    val dirtyData = allRawData.getOrElse(datasetName, "unknown") //"data.hosp.dirty.10k"
    //val config = ConfigFactory.load()
    val mainSchema = allSchemasByName.getOrElse(datasetName, HospSchema)
    // HospSchema
    val schema = mainSchema.getSchema

    val metadataPath = allMetadataByName.getOrElse(datasetName, "unknown")
    val trainDataPath = allTrainData.getOrElse(datasetName, "unknown")

    SparkLOAN
      .withSparkSession("WRANGLINGDATA") {
      session => {
        val dirtyDF: DataFrame = DataSetCreator
          .createFrame(session, dirtyData, schema: _*)
          .repartition(1)
          .toDF(schema: _*)

        val getTypeByAttrName = udf {
          attr: String => {
            mainSchema.dataTypesDictionary.getOrElse(attr, "unknown")
          }
        }

        val isString = udf {
          attributeName: String => {
            val dataType = mainSchema.dataTypesDictionary.getOrElse(attributeName, "unknown")
            dataType match {
              case "String" => 1
              case _ => 0
            }
          }
        }

        val isMissingValue = udf { value: Boolean => {
          if (value) 1.0 else 0.0
        }
        }

        val attributesDFs: Seq[DataFrame] = schema
          .filterNot(_.equalsIgnoreCase(mainSchema.getRecID))
          .map(attribute => {
            val indexByAttrName = mainSchema.getIndexesByAttrNames(List(attribute)).head
            val flattenDF = dirtyDF
              .select(mainSchema.getRecID, attribute)
              .withColumn("attrName", lit(attribute))
              .withColumn("attr", lit(indexByAttrName))
              .withColumn("attrType", getTypeByAttrName(lit(attribute)))
              .withColumn("isNull", isnull(dirtyDF(attribute)))
              .withColumn("value", dirtyDF(attribute))

            flattenDF
              .select(mainSchema.getRecID, "attrName", "attr", "attrType", "isNull", "value")
          })


        val unionAttributesDF: DataFrame = attributesDFs
          .reduce((df1, df2) => df1.union(df2))
          .repartition(1)
          .toDF(Cells.recid, "attrName", Cells.attrnr, "attrType", "isNull", "value")

        println(s"count: ${unionAttributesDF.count()}")

        val metaDF = unionAttributesDF
          .withColumn("missingValue", isMissingValue(unionAttributesDF("isNull")))

        val typeIndexer = new StringIndexer()
          .setInputCol("attrType")
          .setOutputCol("attrTypeIndex")
        val indexedTypesDF = typeIndexer.fit(metaDF).transform(metaDF)

        val typeEncoder = new OneHotEncoder()
          .setDropLast(false)
          .setInputCol("attrTypeIndex")
          .setOutputCol("attrTypeVector")
        val dataTypesEncodedDF = typeEncoder.transform(indexedTypesDF)
        //dataTypesEncodedDF.where(dataTypesEncodedDF("isNull") === true).show(200, false)
        dataTypesEncodedDF.printSchema()

        val top10Values = new MetadataCreator()
          .extractTop10Values(session, metadataPath)
          .cache()
          .toDF("attrNameMeta", "top10")

        val top10List: List[String] = top10Values.select("top10").rdd.map(row => row.getAs[String](0)).collect().toList

        val isTop10Values = udf {
          (value: String, attrName: String) => {
            top10List.contains(value) match {
              case true => 1.0
              case false => 0.0
            }
          }
        }

        val withTop10MetadataDF = dataTypesEncodedDF
          .withColumn("isTop10", isTop10Values(dataTypesEncodedDF("value"), dataTypesEncodedDF("attrName")))

        val assembler = new VectorAssembler()
          .setInputCols(Array("missingValue", "attrTypeVector", "isTop10"))
          .setOutputCol("metadata")

        val metadata = assembler.transform(withTop10MetadataDF)

        metadata.show(false)


        val trainDF = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*)
        trainDF.show(false)

        val toolsAndMetadataDF = trainDF.join(metadata,
          trainDF(FullResult.recid) === metadata(FullResult.recid)
            && trainDF(FullResult.attrnr) === metadata(FullResult.attrnr))
          .show(false)

        toolsAndMetadataDF


      }
    }
  }
}
