package de.experiments.features.error.prediction

import de.evaluation.f1.FullResult
import de.evaluation.util.DataSetCreator
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.generation.FeaturesGenerator
import de.experiments.models.combinator.{Bagging, Stacking}
import de.model.util.Features
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}


class ErrorsPredictor extends ExperimentsCommonConfig {
  private var tools: Seq[String] = FullResult.tools
  private var dataset: String = ""

  def onTools(t: Seq[String]): this.type = {
    tools = t
    this
  }

  def onDataset(d: String): this.type = {
    dataset = d
    this
  }

  def createTrainAndTestData(session: SparkSession): (DataFrame, DataFrame) = {
    val trainDataPath = allTrainData.getOrElse(dataset, "unknown")
    val testDataPath = allTestData.getOrElse(dataset, "unknown")
    // val allFDs = fdsDictionary.allFDs
    val generator = FeaturesGenerator()

    val dirtyDF: DataFrame = generator
      .onDatasetName(dataset)
      .onTools(tools)
      .getDirtyData(session)
      .cache()

    //Set of content-based metadata, such as "attrName", "attrType", "isNull", "missingValue", "attrTypeIndex", "attrTypeVector", "isTop10"
    var contentBasedFeaturesDF: DataFrame = generator.plgrd_generateContentBasedMetadata(session, dirtyDF)
    //todo: extending with complimentary isTop10 -> inTail
    val is_value_in_tail = udf {
      isTop10Value: Double =>
        isTop10Value match {
          case 1.0 => 0.0
          case 0.0 => 1.0
        }
    }
    contentBasedFeaturesDF = contentBasedFeaturesDF
      .withColumn("inTail", is_value_in_tail(contentBasedFeaturesDF("isTop10")))
      .drop(FullResult.value) //to remove double columns -> we avoid future confusion;

    contentBasedFeaturesDF.printSchema()

    //general info about fd-awarnes of each cell
    val generalInfoDF = generator.oneFDTwoFeatureVectors_generateFDsMetadata(session, dirtyDF, generator.allFDs)

    val allMetadata: DataFrame = contentBasedFeaturesDF
      .join(generalInfoDF, Seq(FullResult.recid, FullResult.attrnr)) //todo: joining columns influence several other columns like isMissing

    println("allMetadata:")
    allMetadata.printSchema()

    var trainSystemsAndLabel: DataFrame = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*).cache()
    var testSystemsAndLabel: DataFrame = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*).cache()

    import org.apache.spark.sql.functions._
    val convert_to_double = udf {
      value: String => value.toDouble
    }

    trainSystemsAndLabel = trainSystemsAndLabel
      .withColumn(s"${FullResult.label}-tmp", convert_to_double(trainSystemsAndLabel(FullResult.label)))
      .drop(FullResult.label)
      .withColumnRenamed(s"${FullResult.label}-tmp", FullResult.label)

    testSystemsAndLabel = testSystemsAndLabel
      .withColumn(s"${FullResult.label}-tmp", convert_to_double(testSystemsAndLabel(FullResult.label)))
      .drop(FullResult.label)
      .withColumnRenamed(s"${FullResult.label}-tmp", FullResult.label)

    FullResult.tools.foreach(tool => {

      trainSystemsAndLabel = trainSystemsAndLabel
        .withColumn(s"$tool-tmp", convert_to_double(trainSystemsAndLabel(tool)))
        .drop(tool)
        .withColumnRenamed(s"$tool-tmp", tool)

      testSystemsAndLabel = testSystemsAndLabel
        .withColumn(s"$tool-tmp", convert_to_double(testSystemsAndLabel(tool)))
        .drop(tool)
        .withColumnRenamed(s"$tool-tmp", tool)
    })

    var trainSystemsAndMetaDF = trainSystemsAndLabel
      .join(allMetadata, Seq(FullResult.recid, FullResult.attrnr))

    val allAttrTypes: Seq[String] = generator.getAllDataTypes.map(t => s"$t-type").toSeq
    val metadataColumns = Seq("missingValue", "isTop10", "inTail") ++ allAttrTypes

    val lhs = generator.allFDs.map(fd => s"LHS-${fd.toString}")
    val rhs = generator.allFDs.map(fd => s"RHS-${fd.toString}")
    val fds: List[String] = lhs ++ rhs

    val allTools = FullResult.tools

    val features: Seq[String] = metadataColumns ++ fds ++ allTools

    val allFeaturesAndIds: Seq[String] = features ++ Seq(FullResult.recid, FullResult.attrnr, FullResult.value)

    val featuresAssembler = new VectorAssembler()
      .setInputCols(features.toArray)
      .setOutputCol(Features.featuresCol)

    trainSystemsAndMetaDF = trainSystemsAndMetaDF
      .select(FullResult.label, features: _*)

    trainSystemsAndMetaDF = featuresAssembler
      .transform(trainSystemsAndMetaDF)
      .drop(features: _*)

    val testSysAndLabsDF = testSystemsAndLabel
      .join(allMetadata, Seq(FullResult.recid, FullResult.attrnr))

    var testSystemsAndMetaDF = testSysAndLabsDF
      //          .select(FullResult.label, features: _*)
      .select(FullResult.label, allFeaturesAndIds: _*)

    testSystemsAndMetaDF = featuresAssembler
      .transform(testSystemsAndMetaDF)
      .drop(features: _*)

    (trainSystemsAndMetaDF, testSystemsAndMetaDF)
  }

  def runPredictionWithStacking(session: SparkSession): DataFrame = {

    val testAndTrain = createTrainAndTestData(session)
    val trainSystemsAndMetaDF = testAndTrain._1
    val testSystemsAndMetaDF = testAndTrain._2

    //Run aggregation.
    val stacking = new Stacking()
    val errorsDF = stacking.runStackingOnToolsAndMetadata(session, trainSystemsAndMetaDF, testSystemsAndMetaDF)
    errorsDF

  }

  def runPredictionWithBagging(session: SparkSession): DataFrame = {
    val testAndTrain = createTrainAndTestData(session)
    val trainSystemsAndMetaDF = testAndTrain._1
    val testSystemsAndMetaDF = testAndTrain._2

    val bagging = new Bagging()
    val errorsWithBagging: DataFrame = bagging
      .runBaggingOnToolsAndMetadata(session, trainSystemsAndMetaDF, testSystemsAndMetaDF)
    errorsWithBagging
  }

}

object ErrorsPredictor {
  def apply(): ErrorsPredictor = new ErrorsPredictor()
}
