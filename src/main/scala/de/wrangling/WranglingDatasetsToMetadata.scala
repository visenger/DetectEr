package de.wrangling

import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.{Cells, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.FormatUtil
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by visenger on 07/04/17.
  */
class WranglingDatasetsToMetadata extends Serializable with ExperimentsCommonConfig {

  private var datasetName = ""
  private var dirtyData = ""
  private var mainSchema: Schema = null
  private var schema: Seq[String] = Seq()
  private var metadataPath = ""
  private var trainDataPath = ""
  private var testDataPath = ""

  private var allTools = FullResult.tools

  def onTools(tools: Seq[String]): this.type = {
    allTools = tools
    this
  }


  def onDatasetName(name: String): this.type = {
    datasetName = name
    dirtyData = allRawData.getOrElse(datasetName, "unknown")
    mainSchema = allSchemasByName.getOrElse(datasetName, HospSchema)
    schema = mainSchema.getSchema

    metadataPath = allMetadataByName.getOrElse(datasetName, "unknown")
    trainDataPath = allTrainData.getOrElse(datasetName, "unknown")
    testDataPath = allTestData.getOrElse(datasetName, "unknown")
    this
  }

  /**
    * @return (train: DataFrame, test: DataFrame) each DataFrame formatted as the following schema:
    *         +-----+---------------------------------------------+
    *         |label|features                                     |
    *         +-----+---------------------------------------------+
    *
    **/
  def createMetadataFeatures(session: SparkSession): (DataFrame, DataFrame) = {

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

    //dataTypesEncodedDF.printSchema()

    val top10Values = new MetadataCreator()
      .extractTop10Values(session, metadataPath)
      .cache()
      .toDF("attrNameMeta", "top10")

    val top10List: List[String] = top10Values
      .select("top10")
      .rdd
      .map(row => row.getAs[String](0))
      .collect()
      .toList

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
    // metadata.show(false)

    val trainDF = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*)
    val testDF = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*)
    // trainDF.show(false)

    val trainToolsAndMetadataDF = trainDF.join(metadata,
      trainDF(FullResult.recid) === metadata(FullResult.recid)
        && trainDF(FullResult.attrnr) === metadata(FullResult.attrnr))

    val testToolsAndMetadataDF = testDF.join(metadata,
      testDF(FullResult.recid) === metadata(FullResult.recid)
        && testDF(FullResult.attrnr) === metadata(FullResult.attrnr))


    val transformToToolsVector = udf {
      (tools: mutable.WrappedArray[String]) => {
        val values = tools.map(t => t.toDouble).toArray
        Vectors.dense(values)
      }
    }

    //val tools = FullResult.tools
    val tools = allTools
    val trainToolsCols = tools.map(t => trainDF(t)).toArray
    val testToolsCols = tools.map(t => testDF(t)).toArray

    val trainToolsVectorDF = trainToolsAndMetadataDF
      .withColumn("tools-vector", transformToToolsVector(array(trainToolsCols: _*)))
    val testToolsVectorDF = testToolsAndMetadataDF
      .withColumn("tools-vector", transformToToolsVector(array(testToolsCols: _*)))

    val toolsAndMetadataAssembler = new VectorAssembler()
      .setInputCols(Array("tools-vector", "metadata"))
      .setOutputCol("features")

    val trainFullFeaturesDF = toolsAndMetadataAssembler.transform(trainToolsVectorDF)
    val testFullFeaturesDF = toolsAndMetadataAssembler.transform(testToolsVectorDF)
    // trainFullFeaturesDF.show(200, false)

    //TODO: "features" type is org.apache.spark.ml.linalg.Vector
    val train = trainFullFeaturesDF.select(FullResult.label, "features")
    val test = testFullFeaturesDF.select(FullResult.label, "features")
    (train, test)
  }

}

object WranglingDatasetRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    val datasetName = "salaries"
    val dirtyData = allRawData.getOrElse(datasetName, "unknown")
    val mainSchema = allSchemasByName.getOrElse(datasetName, HospSchema)
    val schema = mainSchema.getSchema

    val metadataPath = allMetadataByName.getOrElse(datasetName, "unknown")
    val trainDataPath = allTrainData.getOrElse(datasetName, "unknown")
    val testDataPath = allTestData.getOrElse(datasetName, "unknown")

    SparkLOAN
      .withSparkSession("WRANGLINGDATA") {
        session => {
          import session.implicits._
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

          //dataTypesEncodedDF.printSchema()

          val top10Values = new MetadataCreator()
            .extractTop10Values(session, metadataPath)
            .cache()
            .toDF("attrNameMeta", "top10")

          val top10List: List[String] = top10Values
            .select("top10")
            .rdd
            .map(row => row.getAs[String](0))
            .collect()
            .toList

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
          // metadata.show(false)

          val trainDF = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*)
          val testDF = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*)
          // trainDF.show(false)

          val trainToolsAndMetadataDF = trainDF.join(metadata,
            trainDF(FullResult.recid) === metadata(FullResult.recid)
              && trainDF(FullResult.attrnr) === metadata(FullResult.attrnr))

          val testToolsAndMetadataDF = testDF.join(metadata,
            testDF(FullResult.recid) === metadata(FullResult.recid)
              && testDF(FullResult.attrnr) === metadata(FullResult.attrnr))


          val transformToToolsVector = udf {
            (tools: mutable.WrappedArray[String]) => {
              val values = tools.map(t => t.toDouble).toArray
              Vectors.dense(values)
            }
          }

          val trainToolsCols = FullResult.tools.map(t => trainDF(t)).toArray
          val testToolsCols = FullResult.tools.map(t => testDF(t)).toArray

          val trainToolsVectorDF = trainToolsAndMetadataDF
            .withColumn("tools-vector", transformToToolsVector(array(trainToolsCols: _*)))
          val testToolsVectorDF = testToolsAndMetadataDF
            .withColumn("tools-vector", transformToToolsVector(array(testToolsCols: _*)))

          val toolsAndMetadataAssembler = new VectorAssembler()
            .setInputCols(Array("tools-vector", "metadata"))
            .setOutputCol("features")

          val trainFullFeaturesDF = toolsAndMetadataAssembler.transform(trainToolsVectorDF)
          // trainFullFeaturesDF.show(200, false)


          val trainLabAndFeatDF = trainFullFeaturesDF.select(FullResult.label, "features")
          val trainLabPointRDD = FormatUtil.prepareDFToLabeledPointRDD(session, trainLabAndFeatDF)

          //APPLY CLASSIFICATION
          //start: decision tree

          val numClasses = 2
          val toolsNum = getFeaturesNumber(trainFullFeaturesDF)
          val categoricalFeaturesInfo = (0 until toolsNum)
            .map(attr => attr -> numClasses).toMap // Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2, 6 -> 2, 7 -> 2)
          val impurity = "gini"
          val maxDepth = toolsNum
          val maxBins = 32

          val decisionTreeModel: DecisionTreeModel = DecisionTree
            .trainClassifier(trainLabPointRDD, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

          //finish: decision tree
          val predictByDT = udf { features: org.apache.spark.mllib.linalg.Vector => decisionTreeModel.predict(features) }

          val testFullFeaturesDF = toolsAndMetadataAssembler.transform(testToolsVectorDF)
          val testLabAndFeatDF = testFullFeaturesDF.select(FullResult.label, "features")
          val testLabAndFeatures = FormatUtil
            .prepareDFToLabeledPointRDD(session, testLabAndFeatDF)
            .toDF(FullResult.label, "features")

          val dtResult = testLabAndFeatures.withColumn("dt-prediction", predictByDT(testLabAndFeatures("features")))
          val dtPredictionAndLabel = FormatUtil.getPredictionAndLabel(dtResult, "dt-prediction")
          val dtEval = F1.evalPredictionAndLabels(dtPredictionAndLabel)
          dtEval.printResult(s"decision tree on $datasetName with metadata")


        }
      }
  }

  private def getFeaturesNumber(trainFullFeaturesDF: DataFrame): Int = {
    trainFullFeaturesDF.select("features").head().getAs[org.apache.spark.ml.linalg.SparseVector](0).size
  }
}
