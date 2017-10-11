package de.experiments.features.descriptive.statistics

import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.FullResult
import de.evaluation.util.DataSetCreator
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.generation.FeaturesGenerator
import de.experiments.metadata.Metadata
import de.experiments.metadata.Metadata._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * We want to know everything about our data. Metadata features for error detection.
  * Why the metadata helps? Which metadata helps?
  **/
class FeaturesDescriptiveStatistics() extends Serializable with ExperimentsCommonConfig {


  private var dataset = ""
  private var tools = FullResult.tools

  private var generator = FeaturesGenerator.init
  private var mainSchema: Schema = null

  private var trainDataPath = ""

  def get(): this.type = {
    generator = generator
      .onDatasetName(dataset)
      .onTools(tools)
    this
  }

  def onDatasetName(ds: String): this.type = {
    dataset = ds
    trainDataPath = allTrainData.getOrElse(dataset, "unknown")
    mainSchema = allSchemasByName.getOrElse(ds, HospSchema)
    this
  }

  def onTools(ts: Seq[String]): this.type = {
    tools = ts
    this
  }

  def getSchema = mainSchema

  def createAllMetadataForTrain(session: SparkSession): DataFrame = {

    // val allFDs = fdsDictionary.allFDs

    val dirtyDF: DataFrame = generator.getDirtyData(session).cache()
    //Set of content-based metadata, such as "attrName", "attrType", "isNull", "missingValue", "attrTypeIndex", "attrTypeVector", "isTop10"
    val contentBasedFeaturesDF: DataFrame = generator.plgrd_generateContentBasedMetadata(session, dirtyDF)

    //general info about fd-awarnes of each cell
    val generalInfoDF = generator.oneFDOneFeature_generateFDsMetadata(session, dirtyDF, generator.allFDs)

    var oneFDTwoVecsDF = generator.oneFDTwoFeatureVectors_generateFDsMetadata(session, dirtyDF, generator.allFDs)

    val fdsCols: Seq[String] = createTwoSidedFDFeatures()

    val columns = Seq(FullResult.attrnr) ++ fdsCols
    oneFDTwoVecsDF = oneFDTwoVecsDF.select(FullResult.recid, columns: _*)

    val allMetadata: DataFrame = contentBasedFeaturesDF
      .join(generalInfoDF, Seq(FullResult.recid, FullResult.attrnr)) //todo: joining columns influence several other columns like isMissing
      .join(oneFDTwoVecsDF, Seq(FullResult.recid, FullResult.attrnr)) //todo: joining columns influence several other columns like isMissing


    //    val testDataPath = allTestData.getOrElse(dataset, "unknown")

    var trainSystemsAndLabel: DataFrame = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*).cache()
    //    var testSystemsAndLabel: DataFrame = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*).cache()

    //todo: convert all str columns into double

    import org.apache.spark.sql.functions._
    val convert_to_double = udf {
      value: String => value.toDouble
    }

    trainSystemsAndLabel = trainSystemsAndLabel
      .withColumn(s"${FullResult.label}-tmp", convert_to_double(trainSystemsAndLabel(FullResult.label)))
      .drop(FullResult.label)
      .withColumnRenamed(s"${FullResult.label}-tmp", FullResult.label)

    //    testSystemsAndLabel = testSystemsAndLabel
    //      .withColumn(s"${FullResult.label}-tmp", convert_to_double(testSystemsAndLabel(FullResult.label)))
    //      .drop(FullResult.label)
    //      .withColumnRenamed(s"${FullResult.label}-tmp", FullResult.label)

    FullResult.tools.foreach(tool => {

      trainSystemsAndLabel = trainSystemsAndLabel
        .withColumn(s"$tool-tmp", convert_to_double(trainSystemsAndLabel(tool)))
        .drop(tool)
        .withColumnRenamed(s"$tool-tmp", tool)

      //      testSystemsAndLabel = testSystemsAndLabel
      //        .withColumn(s"$tool-tmp", convert_to_double(testSystemsAndLabel(tool)))
      //        .drop(tool)
      //        .withColumnRenamed(s"$tool-tmp", tool)
    })


    var trainSystemsAndMetaDF = trainSystemsAndLabel.join(allMetadata, Seq(FullResult.recid, FullResult.attrnr))


    //todo: these are control-columns. Should have zero-MI with other columns.


    //todo: create notTop10 -> meaning the value is placed in the tail of the data histogram -> might be an oulier?!

    val is_value_in_tail = udf {
      isTop10Value: Double =>
        isTop10Value match {
          case 1.0 => 0.0
          case 0.0 => 1.0
        }
    }

    trainSystemsAndMetaDF = trainSystemsAndMetaDF
      .withColumn("inTail", is_value_in_tail(trainSystemsAndMetaDF(topTen)))

    trainSystemsAndMetaDF
  }

  def getAllMetadataFeatures(): Seq[String] = {
    val fdsCols: Seq[String] = createTwoSidedFDFeatures()
    val allAttrTypes: Seq[String] = generator.getAllDataTypes.map(t => s"$t-type").toSeq
    val metadataColumns = Metadata.allMetadata ++ allAttrTypes
    val fds: List[String] = generator.allFDs.map(_.toString)
    val metadataCols: Seq[String] = metadataColumns ++ fds ++ fdsCols
    metadataCols
  }

  def createTwoSidedFDFeatures(): Seq[String] = {
    val lhs = generator.allFDs.map(fd => s"LHS-${fd.toString}")
    val rhs = generator.allFDs.map(fd => s"RHS-${fd.toString}")
    val fdsCols: Seq[String] = lhs ++ rhs
    fdsCols
  }

  def createElementsListForAttribute(metaDF: DataFrame, attr: String): DataFrame = {

    val attributeSpecificDF = metaDF
      .filter(metaDF("attributeName") === attr && metaDF(FullResult.label) === 1.0)
      .toDF()
    attributeSpecificDF
  }


}

object FeaturesDescriptiveStatistics {
  def init: FeaturesDescriptiveStatistics = new FeaturesDescriptiveStatistics()
}
