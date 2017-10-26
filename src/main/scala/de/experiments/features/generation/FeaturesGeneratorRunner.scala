package de.experiments.features.generation

import de.evaluation.f1.{Eval, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.features.prediction.FeaturesPredictivityRunner.{allTestData, allTrainData, computeMutualInformation}
import de.experiments.features.prediction.MutualInformation
import de.experiments.models.combinator.{Bagging, Stacking}
import de.model.util.Features
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

object FeaturesGeneratorRunner {

  def main(args: Array[String]): Unit = {
    val datasets = Seq(/*"blackoak", "hosp", "salaries",*/ "flights")

    datasets.foreach(dataset => {
      //      singleRun(dataset)
      //runOnOneFD_OneFeature(dataset) //todo: need to incorporate   val allFeaturesAndIds: Seq[String] = features ++ Seq(FullResult.recid, FullResult.attrnr) like in runOnOneFD_TwoFeatures(dataset)
      runOnOneFD_TwoFeatures(dataset)
    })

  }

  val threshold = 0.0


  def runOnOneFD_TwoFeatures(dataset: String, tools: Seq[String] = FullResult.tools): Unit = {

    SparkLOAN.withSparkSession("METADATA-ONE-FD-ONE-FEATURE") {
      session => {
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

        //general info about fd-awarnes of each cell
        val generalInfoDF = generator.oneFDTwoFeatureVectors_generateFDsMetadata(session, dirtyDF, generator.allFDs)

        val allMetadata: DataFrame = contentBasedFeaturesDF
          .join(generalInfoDF, Seq(FullResult.recid, FullResult.attrnr)) //todo: joining columns influence several other columns like isMissing

        val trainDataPath = allTrainData.getOrElse(dataset, "unknown")
        val testDataPath = allTestData.getOrElse(dataset, "unknown")

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

        val allFeaturesAndIds: Seq[String] = features ++ Seq(FullResult.recid, FullResult.attrnr)

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


        //Run combinations.
        val stacking = new Stacking()
        val evalStacking: Eval = stacking.performStackingOnToolsAndMetadata(session, trainSystemsAndMetaDF, testSystemsAndMetaDF)
        evalStacking.printResult(s"one-fd-two-feature-vectors: STACKING on $dataset")

        //        todo: proof of concept
        val bagging = new Bagging()
        val evalBagging: Eval = bagging.performBaggingOnToolsAndMetadata(session, trainSystemsAndMetaDF, testSystemsAndMetaDF)
        evalBagging.printResult(s"one-fd-two-feature-vectors: BAGGING on $dataset")
      }
    }


  }

  @deprecated(" test data need to incorporate this columns val allFeaturesAndIds: Seq[String] = features ++ Seq(FullResult.recid, FullResult.attrnr)")
  def runOnOneFD_OneFeature(dataset: String, tools: Seq[String] = FullResult.tools): Unit = {

    SparkLOAN.withSparkSession("METADATA-ONE-FD-ONE-FEATURE") {
      session => {
        // val allFDs = fdsDictionary.allFDs
        val generator = FeaturesGenerator()

        val dirtyDF: DataFrame = generator
          .onDatasetName(dataset)
          .onTools(tools)
          .getDirtyData(session)
          .cache()

        //Set of content-based metadata, such as "attrName", "attrType", "isNull", "missingValue", "attrTypeIndex", "attrTypeVector", "isTop10"
        val contentBasedFeaturesDF: DataFrame = generator.plgrd_generateContentBasedMetadata(session, dirtyDF)

        //general info about fd-awarnes of each cell
        val generalInfoDF = generator.oneFDOneFeature_generateFDsMetadata(session, dirtyDF, generator.allFDs)

        val allMetadata: DataFrame = contentBasedFeaturesDF
          .join(generalInfoDF, Seq(FullResult.recid, FullResult.attrnr)) //todo: joining columns influence several other columns like isMissing

        val trainDataPath = allTrainData.getOrElse(dataset, "unknown")
        val testDataPath = allTestData.getOrElse(dataset, "unknown")

        var trainSystemsAndLabel: DataFrame = DataSetCreator.createFrame(session, trainDataPath, FullResult.schema: _*).cache()
        var testSystemsAndLabel: DataFrame = DataSetCreator.createFrame(session, testDataPath, FullResult.schema: _*).cache()

        //todo: convert all str columns into double

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


        var trainSystemsAndMetaDF = trainSystemsAndLabel.join(allMetadata, Seq(FullResult.recid, FullResult.attrnr))


        //todo: these are control-columns. Should have zero-MI with other columns.
        trainSystemsAndMetaDF = trainSystemsAndMetaDF
          .withColumn("allZeros", lit(0.0))
          .withColumn("allOnes", lit(1.0))

        val is_value_in_tail = udf {
          isTop10Value: Double =>
            isTop10Value match {
              case 1.0 => 0.0
              case 0.0 => 1.0
            }
        }

        trainSystemsAndMetaDF = trainSystemsAndMetaDF
          .withColumn("inTail", is_value_in_tail(trainSystemsAndMetaDF("isTop10")))

        val allAttrTypes: Seq[String] = generator.getAllDataTypes.map(t => s"$t-type").toSeq
        val metadataColumns = Seq("missingValue", "isTop10", "inTail", "allZeros", "allOnes") ++ allAttrTypes
        val fds: List[String] = generator.allFDs.map(_.toString)
        val allTools = FullResult.tools
        val labelItself: Seq[String] = Seq(FullResult.label)

        //        println("CORRELATIONS")
        //        val coef = "pearson"
        //        //val coef = "spearman"
        //        val labels: RDD[Double] = trainSystemsAndMetaDF
        //          .select(FullResult.label)
        //          .rdd
        //          .map(row => row.getDouble(0))
        //        val correlationsWithFDs: Seq[Correlation] = generator.allFDs.map(fd => {
        //          val fdPresence: RDD[Double] = trainSystemsAndMetaDF.select(s"${fd.toString}").rdd.map(row => row.getDouble(0))
        //          val correlation: Double = Statistics.corr(labels, fdPresence, coef)
        //          Correlation(FullResult.label, fd.toString, NumbersUtil.round(correlation, 4))
        //
        //        })
        //
        //
        //        val metadataCorrelations = metadataColumns.map(metadata => {
        //          val metaInfo: RDD[Double] = trainSystemsAndMetaDF.select(metadata).rdd.map(row => row.getDouble(0))
        //          val correlation: Double = Statistics.corr(labels, metaInfo, coef)
        //          Correlation(FullResult.label, metadata, NumbersUtil.round(correlation, 4))
        //        })
        //
        //        val correlationsWithTools: Seq[Correlation] = FullResult.tools.map(tool => {
        //          val metaInfo: RDD[Double] = trainSystemsAndMetaDF.select(tool).rdd.map(row => row.getDouble(0))
        //          val correlation: Double = Statistics.corr(labels, metaInfo, coef)
        //          Correlation(FullResult.label, tool, NumbersUtil.round(correlation, 4))
        //
        //        })
        //
        //
        //        Seq(correlationsWithFDs ++ metadataCorrelations ++ correlationsWithTools)
        //          .flatten
        //          .foreach(cor => println(cor.toString))
        //
        //        println("PAIRWISE DEPENDENCE PROBABILITIES")
        //
        //
        //        val dfsDependenceProbs: Seq[DependenceProbability] = computePairwiseDependenceProbs(trainSystemsAndMetaDF, fds)
        //        val metadataDependenceProbs: Seq[DependenceProbability] = computePairwiseDependenceProbs(trainSystemsAndMetaDF, metadataColumns)
        //        val toolsDependenceProbs: Seq[DependenceProbability] = computePairwiseDependenceProbs(trainSystemsAndMetaDF, allTools)
        //        val labelToItselfProb: Seq[DependenceProbability] = computePairwiseDependenceProbs(trainSystemsAndMetaDF, labelItself)
        //
        //        Seq(dfsDependenceProbs ++ metadataDependenceProbs ++ toolsDependenceProbs ++ labelToItselfProb)
        //          .flatten
        //          .foreach(p => println(p.toString))
        //
        println("MUTUAL INFORMATION")

        val fdsMIs: Seq[MutualInformation] = computeMutualInformation(trainSystemsAndMetaDF, fds)
        val metadataMIs: Seq[MutualInformation] = computeMutualInformation(trainSystemsAndMetaDF, metadataColumns)
        val toolsMIs: Seq[MutualInformation] = computeMutualInformation(trainSystemsAndMetaDF, allTools)
        // val labelToItselfMI: Seq[MutualInformation] = computeMutualInformation(trainSystemsAndMetaDF, labelItself)

        //----------------- todo: Prepare for combinations

        val allMIs = Seq(fdsMIs ++ metadataMIs ++ toolsMIs).flatten
        allMIs.foreach(mi => println(mi.toString))

        val relevantFeatures: Seq[String] = allMIs
          .filter(featureMI => featureMI.mi > threshold)
          .map(featureMI => featureMI.column2)
        println(s"relevant features: ${relevantFeatures.mkString(", ")}")

        val featuresAssembler = new VectorAssembler()
          .setInputCols(relevantFeatures.toArray)
          .setOutputCol(Features.featuresCol)

        trainSystemsAndMetaDF = trainSystemsAndMetaDF
          .select(FullResult.label, relevantFeatures: _*)

        trainSystemsAndMetaDF = featuresAssembler
          .transform(trainSystemsAndMetaDF)
          .drop(relevantFeatures: _*)

        var testSystemsAndMetaDF = testSystemsAndLabel
          .join(allMetadata, Seq(FullResult.recid, FullResult.attrnr))
          .select(FullResult.label, relevantFeatures: _*)

        testSystemsAndMetaDF = featuresAssembler
          .transform(testSystemsAndMetaDF)
          .drop(relevantFeatures: _*)

        //Run combinations.
        val stacking = new Stacking()
        val evalStacking: Eval = stacking.performStackingOnToolsAndMetadata(session, trainSystemsAndMetaDF, testSystemsAndMetaDF)
        evalStacking.printResult(s"one-fd-one-feature-vector: STACKING on $dataset")

        val bagging = new Bagging()
        val evalBagging: Eval = bagging.performBaggingOnToolsAndMetadata(session, trainSystemsAndMetaDF, testSystemsAndMetaDF)
        evalBagging.printResult(s"one-fd-one-feature-vector: BAGGING on $dataset")
      }
    }


  }


  def singleRun(dataset: String, tools: Seq[String] = FullResult.tools): Unit = {

    SparkLOAN.withSparkSession("METADATA-COMBI") {
      session => {
        // val allFDs = fdsDictionary.allFDs
        val generator = FeaturesGenerator()


        val dirtyDF: DataFrame = generator
          .onDatasetName(dataset)
          .onTools(tools)
          .getDirtyData(session)
          .cache()

        //Set of content-based metadata, such as "attrName", "attrType", "isNull", "missingValue", "attrTypeIndex", "attrTypeVector", "isTop10"
        val contentBasedFeaturesDF: DataFrame = generator.generateContentBasedMetadata(session, dirtyDF)

        //FD-partiotion - based features
        val fdMetadataDF: DataFrame = generator.generatePartitionsBased_FDMetadata(session, dirtyDF, generator.allFDs)

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
