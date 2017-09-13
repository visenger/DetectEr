package de.experiments.features.generation

import com.typesafe.config.ConfigFactory
import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.{HospSchema, Schema}
import de.evaluation.f1.{Cells, Eval, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.metadata.{FD, HospFDsDictionary}
import de.experiments.models.combinator.Bagging
import de.model.util.Features
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

class FeaturesGenerator extends Serializable with ExperimentsCommonConfig {

  private var datasetName = ""
  private var dirtyData = ""
  private var mainSchema: Schema = null
  private var schema: Seq[String] = Seq()
  private var metadataPath = ""
  private var trainDataPath = ""
  private var testDataPath = ""


  var allFDs: List[FD] = null

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

    allFDs = allFDsDictionariesByName.getOrElse(datasetName, HospFDsDictionary).allFDs

    this
  }

  def getDirtyData(session: SparkSession): DataFrame = {
    val dirtyDF = DataSetCreator
      .createFrame(session, dirtyData, schema: _*).cache()
    dirtyDF
  }

  def generateContentBasedMetadata(session: SparkSession, dirtyDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    val getTypeByAttrName = udf {
      attr: String => {
        mainSchema.dataTypesDictionary.getOrElse(attr, "unknown")
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

    val isMissingValue = udf { value: Boolean => {
      if (value) 1.0 else 0.0
    }
    }
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

    //final assember of all content-based metadata.
    val assembler = new VectorAssembler()
      .setInputCols(Array("missingValue", "attrTypeVector", "isTop10"))
      .setOutputCol(Features.contentBasedVector)

    val contentBasedMetadataCols: List[String] = List("attrName", "attrType", "isNull", "missingValue", "attrTypeIndex", "attrTypeVector", "isTop10")
    val contentMetadataDF = assembler.transform(withTop10MetadataDF).drop(contentBasedMetadataCols: _*)
    //contentMetadataDF.show(false)

    contentMetadataDF
  }

  def generateGeneralInfoMetadata(session: SparkSession, dirtyDF: DataFrame, datasetFDs: List[FD]): DataFrame = {
    import org.apache.spark.sql.functions._

    val computeFDAwarnes = udf { attribute: String => {
      val whereTheAttibuteIsAvailable: Int = datasetFDs.count(fd => fd.getFD.contains(attribute))
      whereTheAttibuteIsAvailable.toString
    }
    }

    val attributes: Seq[String] = mainSchema.getSchema.filterNot(_.equals(mainSchema.getRecID))
    val attrs: Seq[DataFrame] = attributes.map(attr => {
      val attrIdx = mainSchema.getIndexesByAttrNames(List(attr)).head
      val attrDF = dirtyDF.select(mainSchema.getRecID, attr)
        .withColumn(FullResult.attrnr, lit(attrIdx))
        .withColumn("fdAware", computeFDAwarnes(lit(attr)))
        .toDF(FullResult.recid, "value", FullResult.attrnr, "fdAware")

      attrDF
        .select(FullResult.recid, FullResult.attrnr, "value", "fdAware")
        .toDF()
    })

    val combinedGeneralInfo = attrs
      .reduce((df1, df2) => df1.union(df2))
      .repartition(1)
      .toDF(FullResult.recid, FullResult.attrnr, "value", "fdAware")


    val indexer = new StringIndexer().setInputCol("fdAware").setOutputCol("general-fd")
    val indexedDF = indexer.fit(combinedGeneralInfo).transform(combinedGeneralInfo).drop("fdAware")
    val encoder = new OneHotEncoder().setDropLast(false).setInputCol("general-fd").setOutputCol(Features.generalVector)
    val encodedDF = encoder.transform(indexedDF).drop("general-fd")
    encodedDF
  }

  def generateFDMetadata(session: SparkSession, dirtyDF: DataFrame, datasetFDs: List[FD]): DataFrame = {
    import org.apache.spark.sql.functions._

    val allFDEncodings: List[DataFrame] = datasetFDs.map(fd => {

      val fd0 = fd.getFD
      val lhs = fd.lhs
      val lhsFD: List[Column] = lhs.map(dirtyDF(_))

      //grouping is done by the LHS of the fd.
      val lhsCounts: DataFrame = dirtyDF
        .groupBy(lhsFD: _*)
        .count()

      val clustersForFD: DataFrame = lhsCounts
        .where(lhsCounts("count") > 1)
        .withColumn("cluster-id", concat_ws("-", lit("clust"), monotonically_increasing_id() + 1))
        .toDF()

      //clustersForFD.printSchema()
      println(s"FD processed: ${fd.toString}")
      println(s"number of clusters for the FD : ${clustersForFD.count()}")

      /*
    *  @param joinType Type of join to perform. Default `inner`. Must be one of:
   *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   *                 `right`, `right_outer`, `left_semi`, `left_anti`.
    * */

      //clust-zero is a placeholder for any attributes, which do not have pairs.(but belong to some fd)
      val defaultValForCells = "clust-zero"
      val joinedWithGroups: DataFrame = dirtyDF
        .join(clustersForFD, lhs, "left_outer")
        .na.fill(0, Seq("count"))
        .na.fill(defaultValForCells, Seq("cluster-id"))

      // joinedWithGroups.show()

      val attributes: Seq[String] = mainSchema.getSchema.filterNot(_.equals(mainSchema.getRecID))

      val cluster_fd = udf {
        (value: String, attrName: String, fd: mutable.WrappedArray[String]) => {
          //all values, not members of any fd will get a default value "no-fd"
          val attrIsNotInFD = "no-fd"
          val valueForFD: String = if (fd.contains(attrName)) value else attrIsNotInFD

          valueForFD
        }
      }
      val attrDFs: Seq[DataFrame] = attributes.map(attr => {
        val attrIdx = mainSchema.getIndexesByAttrNames(List(attr)).head
        val attrDF = joinedWithGroups.select(mainSchema.getRecID, attr, "cluster-id")
          .withColumn(FullResult.attrnr, lit(attrIdx))
          .withColumn("clusters-fd", cluster_fd(joinedWithGroups("cluster-id"), lit(attr), array(fd0.map(lit(_)): _*)))
          .toDF(FullResult.recid, "value", "cluster-id", FullResult.attrnr, "clusters-fd")

        attrDF
          .select(FullResult.recid, FullResult.attrnr, "value", "clusters-fd")
          .toDF()
      })

      val fdIdx = generateFDName(fd)
      val fdsEncoded: DataFrame = attrDFs
        .reduce((df1, df2) => df1.union(df2))
        .repartition(1)
        .toDF(FullResult.recid, FullResult.attrnr, "value", s"fd-${fdIdx}")


      val fdIndexer = new StringIndexer()
        .setInputCol(s"fd-${fdIdx}")
        .setOutputCol(s"fd-${fdIdx}-idx")
      val fdIndexedDF = fdIndexer.fit(fdsEncoded).transform(fdsEncoded).drop(s"fd-${fdIdx}")

      val oneHotEncoderForFD = new OneHotEncoder()
        .setDropLast(false)
        .setInputCol(s"fd-${fdIdx}-idx")
        .setOutputCol(s"fd-${fdIdx}-vec")
      val dfVectorizedDF = oneHotEncoderForFD.transform(fdIndexedDF).drop(s"fd-${fdIdx}-idx")

      dfVectorizedDF
    })

    val joinedFDs = allFDEncodings
      .reduce((fd1, fd2) => fd1.join(fd2, Seq(FullResult.recid, FullResult.attrnr, "value")))
    // joinedFDs.show()

    val fdArray: Array[String] = datasetFDs.map(fd => s"fd-${generateFDName(fd)}-vec").toArray

    val vectorAssembler = new VectorAssembler()
      .setInputCols(fdArray)
      .setOutputCol(Features.dependenciesVector) //all encodings for the functional dependencies

    val fdsDataframe = vectorAssembler.transform(joinedFDs).drop(fdArray: _*)
    fdsDataframe
  }

  def accumulateAllFeatures(session: SparkSession, allMetaDFs: Seq[DataFrame]): DataFrame = {

    val fullMetadataDF = allMetaDFs.tail
      .foldLeft(allMetaDFs.head)((acc, df) => acc.join(df, Seq(Cells.recid, Cells.attrnr, "value")))
    //val fullMetadataDF = fdsDataframe.join(contentMetadataDF, Seq(Cells.recid, Cells.attrnr, "value"))

    val allMetadataCols = allMetaDFs.flatMap(df => {
      df.columns.intersect(Features.allFeatures)
    }).toArray

    //println(s"all metadata contains the following columns: ${allMetadataCols.mkString(", ")}")

    //val allMetadataCols = Array("fds", "metadata")

    val fullVecAssembler = new VectorAssembler()
      .setInputCols(allMetadataCols)
      .setOutputCol(Features.fullMetadata)

    val metadataDF = fullVecAssembler
      .transform(fullMetadataDF)
      .drop(allMetadataCols: _*)
    metadataDF
  }

  def accumulateDataAndMetadata(session: SparkSession, trainDF: DataFrame, testDF: DataFrame, metadataDF: DataFrame): (DataFrame, DataFrame) = {

    val trainToolsAndMetadataDF = trainDF.join(metadataDF, Seq(FullResult.recid, FullResult.attrnr))

    val testToolsAndMetadataDF = testDF.join(metadataDF, Seq(FullResult.recid, FullResult.attrnr))


    val toolsAndMetadataAssembler = new VectorAssembler()
      .setInputCols(Array(Features.toolsVector, Features.fullMetadata))
      .setOutputCol(Features.featuresCol)

    val colNames = List(FullResult.recid, FullResult.attrnr, "value", Features.toolsVector, Features.fullMetadata)

    val trainFullFeaturesDF = toolsAndMetadataAssembler
      .transform(trainToolsAndMetadataDF)
      .drop(colNames: _*)
      .drop(FullResult.tools: _*)

    val testFullFeaturesDF = toolsAndMetadataAssembler
      .transform(testToolsAndMetadataDF)
      .drop(colNames: _*)
      .drop(FullResult.tools: _*)


    println(s"1. train feature vector ${trainFullFeaturesDF.select(Features.featuresCol).first().getAs[org.apache.spark.ml.linalg.Vector](Features.featuresCol).size}")
    println(s"2. test feature vector ${testFullFeaturesDF.select(Features.featuresCol).first().getAs[org.apache.spark.ml.linalg.Vector](Features.featuresCol).size}")
    /* 1. train feature vector 14706
     2. test feature vector 14705 <- Exception in naive bayes execution for BlackOak dataset.
    */

    (trainFullFeaturesDF, testFullFeaturesDF)


  }

  def createSystemsFeatures(session: SparkSession): (DataFrame, DataFrame) = {
    import org.apache.spark.sql.functions._

    val trainDF = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*).cache()
    val testDF = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*).cache()


    val trainToolsCols: Array[Column] = allTools.map(t => trainDF(t)).toArray
    val testToolsCols: Array[Column] = allTools.map(t => testDF(t)).toArray

    //todo: Select tools for the further processing. -> needed for clustering in the BestK-Systems experiments.


    val transformToToolsVector = udf {
      (tools: mutable.WrappedArray[String]) => {
        val values = tools.map(t => t.toDouble).toArray
        Vectors.dense(values)
      }
    }
    val countToolsUDF = udf {
      (tools: mutable.WrappedArray[String]) => {
        val values = tools.map(t => t.toDouble).toArray
        s"${values.sum}"
      }
    }
    //adding new feature: count tools indicated an error on the respective cell
    val countToolsCol = "count-tools"
    val tmpToolsCols = s"tmp-${Features.toolsVector}"

    val trainCountToolsDF = trainDF.withColumn(countToolsCol, countToolsUDF(array(trainToolsCols: _*)))
    val testCountToolsDF = testDF.withColumn(countToolsCol, countToolsUDF(array(testToolsCols: _*)))

    val indexer = new StringIndexer().setInputCol(countToolsCol).setOutputCol(s"$countToolsCol-idx")
    val trainCountIndxDF: DataFrame = indexer.fit(trainCountToolsDF).transform(trainCountToolsDF).drop(countToolsCol)
    val testCountIndxDF: DataFrame = indexer.fit(testCountToolsDF).transform(testCountToolsDF).drop(countToolsCol)

    val encoder = new OneHotEncoder()
      .setDropLast(false)
      .setInputCol(s"$countToolsCol-idx")
      .setOutputCol(s"$countToolsCol-vec")
    val trainCountVecDF: DataFrame = encoder.transform(trainCountIndxDF).drop(s"$countToolsCol-idx")
    val testCountVecDF: DataFrame = encoder.transform(testCountIndxDF).drop(s"$countToolsCol-idx")


    val trainToolsVectorDF = trainCountVecDF
      .withColumn(tmpToolsCols, transformToToolsVector(array(trainToolsCols: _*)))
    val testToolsVectorDF = testCountVecDF
      .withColumn(tmpToolsCols, transformToToolsVector(array(testToolsCols: _*)))

    val tmpCols = Array(s"$countToolsCol-vec", tmpToolsCols)
    val assembler = new VectorAssembler().setInputCols(tmpCols).setOutputCol(Features.toolsVector)
    val trainVectorDF: DataFrame = assembler.transform(trainToolsVectorDF).drop(tmpCols: _*)
    val testVectorDF: DataFrame = assembler.transform(testToolsVectorDF).drop(tmpCols: _*)

    println(s"1.tools initial train feature vector ${trainVectorDF.select(Features.toolsVector).first().getAs[org.apache.spark.ml.linalg.Vector](Features.toolsVector).size}")
    println(s"2.tools initial test feature vector ${testVectorDF.select(Features.toolsVector).first().getAs[org.apache.spark.ml.linalg.Vector](Features.toolsVector).size}")


    (trainVectorDF, testVectorDF)
  }


  private def generateFDName(fd: FD) = {
    fd.getFD.mkString("").hashCode
  }

}

object FeaturesGenerator {
  def init: FeaturesGenerator = {
    val generator = new FeaturesGenerator()
    generator
  }
}

object FeaturesGeneratorPlayground {

  def main(args: Array[String]): Unit = {
    val experimentsConf = ConfigFactory.load("experiments.conf")
    val datasets = Seq("blackoak" /*, "hosp", "salaries", "flights"*/)

    datasets.foreach(dataset => {
      (2 to 4).foreach(i => {
        // val systems: Seq[String] = experimentsConf.getStringList(s"$dataset.k.$i").asScala.toSeq

        // println(s"running on ${dataset.toUpperCase()} | clusters number k=$i | on systems: ${systems.mkString(", ")}")
        println(s"running on ${dataset.toUpperCase()} ")
        singleRun(dataset, Seq("exists-4", "exists-5", "exists-1", "exists-2", "exists-3"))
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



