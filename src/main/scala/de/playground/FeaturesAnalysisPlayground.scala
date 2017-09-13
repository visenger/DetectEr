package de.playground

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{Eval, FullResult}
import de.evaluation.util.SparkLOAN
import de.experiments.features.generation.FeaturesGenerator
import de.experiments.models.combinator.Bagging
import de.model.util.Features
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg
import org.apache.spark.sql.DataFrame


class FeaturesAnalysisPlayground {

}

object FeaturesAnalyzerPlayground {

  def _main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("TEST-STAT") {
      session => {
        val sc: SparkContext = session.sparkContext
        //
        //        val obs: RDD[LabeledPoint] =
        //          sc.parallelize(
        //            Seq(
        //              LabeledPoint(1.0, org.apache.spark.mllib.linalg.Vectors.dense(1.0, 0.0, 1.0)),
        //              LabeledPoint(1.0, org.apache.spark.mllib.linalg.Vectors.dense(1.0, 1.0, 0.0)),
        //              LabeledPoint(0.0, org.apache.spark.mllib.linalg.Vectors.dense(1.0, 0.0, 0.0)),
        //              LabeledPoint(0.0, org.apache.spark.mllib.linalg.Vectors.dense(0.0, 1.0, 0.0))
        //            )
        //          ) // (feature, label) pairs.
        //
        //        // The contingency table is constructed from the raw (feature, label) pairs and used to conduct
        //        // the independence test. Returns an array containing the ChiSquaredTestResult for every feature
        //        // against the label.
        //        val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
        //        featureTestResults.zipWithIndex.foreach { case (k, v) =>
        //          println("Column " + (v + 1).toString + ":")
        //          println(k)
        //        } // summary of the test
        //
        //
        //        val seriesX: RDD[Double] = sc.parallelize(Array(1, 2, 3, 3, 5)) // a series
        //        // must have the same number of partitions and cardinality as seriesX
        //        val seriesY: RDD[Double] = sc.parallelize(Array(11, 22, 33, 33, 555))
        //
        //        // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a
        //        // method is not specified, Pearson's method will be used by default.
        //        val correlation: Double = Statistics.corr(seriesX, seriesY, "pearson")
        //        println(s"Correlation is: $correlation")
        //
        //        val data: RDD[Vector] = sc.parallelize(
        //          Seq(
        //            org.apache.spark.mllib.linalg.Vectors.dense(1.0, 0.0, 1.0),
        //            org.apache.spark.mllib.linalg.Vectors.dense(1.0, 1.0, 0.0),
        //            org.apache.spark.mllib.linalg.Vectors.dense(1.0, 0.0, 0.0),
        //            org.apache.spark.mllib.linalg.Vectors.dense(0.0, 1.0, 0.0)
        //          )) // note that each Vector is a row and not a column
        //
        //        // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method
        //        // If a method is not specified, Pearson's method will be used by default.
        //        val correlMatrix: Matrix = Statistics.corr(data, "pearson")
        //        println(correlMatrix.toString)

        import org.apache.spark.ml.feature.ChiSqSelector
        import org.apache.spark.ml.linalg.Vectors
        import session.implicits._

        val data1: Seq[(Int, linalg.Vector, Double)] = Seq(
          (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
          (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
          (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
        )

        val df = session.createDataset(data1).toDF("id", "features", "clicked")

        val selector = new ChiSqSelector()
          .setNumTopFeatures(2)
          .setFeaturesCol("features")
          .setLabelCol("clicked")
          .setOutputCol("selectedFeatures")

        val result = selector.fit(df).transform(df)

        println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
        result.show()


      }
    }
  }

  def main(args: Array[String]): Unit = {
    val experimentsConf = ConfigFactory.load("experiments.conf")
    val datasets = Seq(/*"blackoak" ,*/ "hosp" /*, "salaries", "flights"*/)
    datasets.foreach(dataset => {
      // println(s"running on ${dataset.toUpperCase()} | clusters number k=$i | on systems: ${systems.mkString(", ")}")
      println(s"running on ${dataset.toUpperCase()} ")
      singleRun(dataset)
    })


  }

  def singleRun(dataset: String, tools: Seq[String] = FullResult.tools): Unit = {

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

        import org.apache.spark.sql.functions._
        val convert_to_double = udf {
          label: String => label.toDouble
        }

        val trainDFDoubleLabel = fullTrainDF
          .withColumn("double-label", convert_to_double(fullTrainDF("label")))
          .drop("label")
          .withColumnRenamed("double-label", "label")
        val testDFDoubleLabel = fullTestDF
          .withColumn("double-label", convert_to_double(fullTestDF("label")))
          .drop("label")
          .withColumnRenamed("double-label", "label")
        trainDFDoubleLabel.printSchema()


        val selector = new ChiSqSelector()
          .setNumTopFeatures(100)
          .setFeaturesCol(Features.featuresCol)
          // .setLabelCol("label")
          .setOutputCol("selected-features")

        val trainResult: DataFrame = selector
          .fit(trainDFDoubleLabel)
          .transform(trainDFDoubleLabel)


        println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
        trainResult.show()

        val trainChiSq: DataFrame = trainResult
          .drop(Features.featuresCol)
          .withColumnRenamed("selected-features", Features.featuresCol)


        val testResult = selector
          .fit(testDFDoubleLabel)
          .transform(testDFDoubleLabel)

        val testChiSq = testResult
          .drop(Features.featuresCol)
          .withColumnRenamed("selected-features", Features.featuresCol)

        //Run combinations.
        //        val stacking = new Stacking()
        //        val evalStacking: Eval = stacking.performStackingOnToolsAndMetadata(session, fullTrainDF, fullTestDF)
        //        evalStacking.printResult(s"STACKING on $dataset")

        val bagging = new Bagging()
        val evalBagging: Eval = bagging.performBaggingOnToolsAndMetadata(session, trainChiSq, testChiSq)
        evalBagging.printResult(s"BAGGING on $dataset")

      }
    }


  }
}
