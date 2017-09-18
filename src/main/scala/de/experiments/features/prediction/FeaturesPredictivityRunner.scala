package de.experiments.features.prediction

import de.evaluation.f1.{Eval, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.generation.FeaturesGenerator
import de.experiments.models.combinator.{Bagging, Stacking}
import de.model.util.{Features, NumbersUtil}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Detecting predictive relationship between features and the label column.
  * Quantification of the strength relationship: How much information (in the information-
  * theorethical sense), the feature column contains about the labels.
  **/
object FeaturesPredictivityRunner extends ExperimentsCommonConfig {


  def main(args: Array[String]): Unit = {
    val datasets = Seq("blackoak", "hosp", "salaries", "flights")

    datasets.foreach(dataset => {
      println(
        s"""
           |
           |running on ${dataset.toUpperCase()}
           |""".stripMargin)
      computePredictivityOfFeatures(dataset)
    })


  }

  val threshold = 0.0

  def computePredictivityOfFeatures(dataset: String, tools: Seq[String] = FullResult.tools): Unit = {

    SparkLOAN.withSparkSession("METADATA-ANALYSER") {
      session => {
        // val allFDs = fdsDictionary.allFDs
        val generator = FeaturesGenerator.init


        val dirtyDF: DataFrame = generator
          .onDatasetName(dataset)
          .onTools(tools)
          .getDirtyData(session)
          .cache()

        //Set of content-based metadata, such as "attrName", "attrType", "isNull", "missingValue", "attrTypeIndex", "attrTypeVector", "isTop10"
        val contentBasedFeaturesDF: DataFrame = generator.plgrd_generateContentBasedMetadata(session, dirtyDF)

        //general info about fd-awarnes of each cell
        val generalInfoDF = generator.plgrd_generateGeneralInfoMetadata(session, dirtyDF, generator.allFDs)

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

        //systemsAndMetaDF.printSchema()

        val labelAndMIssing: RDD[(Double, Double)] = trainSystemsAndMetaDF
          .select(FullResult.label, "missingValue")
          .rdd.map(row => {
          (row.getDouble(0), row.getDouble(1))
        })
        labelAndMIssing.countByValue().foreach(item => println(s" (${item._1}) count ${item._2}"))


        val allAttrTypes: Seq[String] = generator.getAllDataTypes.map(t => s"$t-type").toSeq
        val metadataColumns = Seq("missingValue", "isTop10", "allZeros", "allOnes") ++ allAttrTypes
        val fds: List[String] = generator.allFDs.map(_.toString)
        val allTools = FullResult.tools
        val labelItself: Seq[String] = Seq(FullResult.label)

        println("CORRELATIONS")
        val coef = "pearson"
        //val coef = "spearman"
        val labels: RDD[Double] = trainSystemsAndMetaDF
          .select(FullResult.label)
          .rdd
          .map(row => row.getDouble(0))
        val correlationsWithFDs: Seq[Correlation] = generator.allFDs.map(fd => {
          val fdPresence: RDD[Double] = trainSystemsAndMetaDF.select(s"${fd.toString}").rdd.map(row => row.getDouble(0))
          val correlation: Double = Statistics.corr(labels, fdPresence, coef)
          Correlation(FullResult.label, fd.toString, NumbersUtil.round(correlation, 4))

        })


        val metadataCorrelations = metadataColumns.map(metadata => {
          val metaInfo: RDD[Double] = trainSystemsAndMetaDF.select(metadata).rdd.map(row => row.getDouble(0))
          val correlation: Double = Statistics.corr(labels, metaInfo, coef)
          Correlation(FullResult.label, metadata, NumbersUtil.round(correlation, 4))
        })

        val correlationsWithTools: Seq[Correlation] = FullResult.tools.map(tool => {
          val metaInfo: RDD[Double] = trainSystemsAndMetaDF.select(tool).rdd.map(row => row.getDouble(0))
          val correlation: Double = Statistics.corr(labels, metaInfo, coef)
          Correlation(FullResult.label, tool, NumbersUtil.round(correlation, 4))

        })


        Seq(correlationsWithFDs ++ metadataCorrelations ++ correlationsWithTools)
          .flatten
          .foreach(cor => println(cor.toString))

        println("PAIRWISE DEPENDENCE PROBABILITIES")


        val dfsDependenceProbs: Seq[DependenceProbability] = computePairwiseDependenceProbs(trainSystemsAndMetaDF, fds)
        val metadataDependenceProbs: Seq[DependenceProbability] = computePairwiseDependenceProbs(trainSystemsAndMetaDF, metadataColumns)
        val toolsDependenceProbs: Seq[DependenceProbability] = computePairwiseDependenceProbs(trainSystemsAndMetaDF, allTools)
        val labelToItselfProb: Seq[DependenceProbability] = computePairwiseDependenceProbs(trainSystemsAndMetaDF, labelItself)

        Seq(dfsDependenceProbs ++ metadataDependenceProbs ++ toolsDependenceProbs ++ labelToItselfProb)
          .flatten
          .foreach(p => println(p.toString))

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
        evalStacking.printResult(s"STACKING on $dataset")

        val bagging = new Bagging()
        val evalBagging: Eval = bagging.performBaggingOnToolsAndMetadata(session, trainSystemsAndMetaDF, testSystemsAndMetaDF)
        evalBagging.printResult(s"BAGGING on $dataset")
      }
    }


  }


  /** Mutual Information: https://nlp.stanford.edu/IR-book/html/htmledition/mutual-information-1.html */
  def computeMutualInformation(systemsAndMetaDF: DataFrame, columns: Seq[String]): Seq[MutualInformation] = {
    //todo 1: select the domain values of each column
    val xs, ys = Seq(1.0, 0.0)

    val total: Double = systemsAndMetaDF.select(FullResult.label).count().toDouble
    val mutualInformations: Seq[MutualInformation] = columns.map(col => {
      val column = col.toString

      val countsTable: Seq[CountsTableRow] = for {x <- xs; y <- ys} yield {

        val df = systemsAndMetaDF.select(FullResult.label, column)
        val xyCount = df
          .where(systemsAndMetaDF(FullResult.label) === x && systemsAndMetaDF(column) === y).count().toDouble
        val xCount = df
          .where(systemsAndMetaDF(FullResult.label) === x).count().toDouble
        val yCount = df
          .where(systemsAndMetaDF(column) === y).count().toDouble


        CountsTableRow(x, y, total, xyCount, xCount, yCount)
      }
      /** Mutual Information formula: https://nlp.stanford.edu/IR-book/html/htmledition/mutual-information-1.html */
      val mutualInformation: Double = countsTable
        .map(row => ((row.xyCount / row.total) * Math.log((row.total * row.xyCount) / (row.xCount * row.yCount))))
        .filterNot(member => member.isNaN)
        .foldLeft(0.0)((acc, item) => acc + item)


      MutualInformation(FullResult.label, column, NumbersUtil.round(mutualInformation, 4))
    })
    mutualInformations

  }


  def computePairwiseDependenceProbs(systemsAndMetaDF: DataFrame, columns: Seq[String]): Seq[DependenceProbability] = {

    def approximateEquals(a: Double, b: Double): Boolean = {
      if ((a - b).abs < 0.000001) true else false
    }

    //todo 1: select the domain values of each column
    val xs, ys = Seq(1.0, 0.0)

    val total: Double = systemsAndMetaDF.select(FullResult.label).count().toDouble

    val probabilitiesOfDependence: Seq[DependenceProbability] = columns.map(col => {
      val column = col.toString


      val allIndependentCases: Seq[Double] = for {x <- xs; y <- ys} yield {

        val df = systemsAndMetaDF.select(FullResult.label, column)
        val xyCount = df
          .where(systemsAndMetaDF(FullResult.label) === x && systemsAndMetaDF(column) === y).count().toDouble
        val xCount = df
          .where(systemsAndMetaDF(FullResult.label) === x).count().toDouble
        val yCount = df
          .where(systemsAndMetaDF(column) === y).count().toDouble


        val jointProb = xyCount / total

        val productOfMarginals = (xCount / total) * (yCount / total)

        //From the definition of independence: P(A,B)=P(A)*P(B)
        if (approximateEquals(jointProb, productOfMarginals)) println(s"the independence criteria mathch for label and $column : $jointProb == $productOfMarginals")

        val dependence: Double = approximateEquals(jointProb, productOfMarginals) match {
          case true => 1.0
          case false => 0.0
        }
        dependence

      }
      val totalIndependence = allIndependentCases.filterNot(_.isNaN).sum

      val totalCases = 4.0
      val probOfDependence: Double = 1 - totalIndependence / totalCases

      DependenceProbability(FullResult.label, column, NumbersUtil.round(probOfDependence, 4))

    })
    probabilitiesOfDependence

  }
}
