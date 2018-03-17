package de.experiments.features.error.prediction

import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.DataFrame

object ErrorPredictorRunner {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("Error Prediction Run") {
      session => {
        val datasets = Seq("blackoak", "hosp", "salaries", "flights")

        datasets.foreach(dataset => {
          val predictor = ErrorsPredictor()
          predictor.onDataset(dataset)
          predictor.evaluateBagging(session)
          predictor.evaluateStacking(session)
        })


      }
    }
  }

}

object BaggingExperiment {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("Error Prediction Run") {
      session => {
        val datasets = Seq(/*"blackoak",*/ "hosp" /*, "salaries", "flights"*/)

        datasets.foreach(dataset => {
          val predictor = ErrorsPredictor()
          predictor.onDataset(dataset)
          val result: DataFrame = predictor.runPredictionWithBagging(session)
          result.show(45, false)

//          val pathToData = "/Users/visenger/research/datasets/clean-and-dirty-data/HOSP-10K/matrix-with-prediction"
//
//          result
//            .repartition(1)
//            .write
//            .option("header", "true")
//            .csv(pathToData)

        })


      }
    }
  }

}


object DataMatrixPersister {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("PERSISTER") {
      session => {
        val datasets = Seq("hosp", "salaries", "flights")

        val pathToData = Map(
          "hosp" -> "/Users/visenger/research/datasets/clean-and-dirty-data/HOSP-10K/matrix",
          "salaries" -> "/Users/visenger/research/datasets/clean-and-dirty-data/SALARIES/salaries_small/matrix",
          "flights" -> "/Users/visenger/research/datasets/clean-and-dirty-data/FLIGHTS/matrix")

        datasets.foreach(dataset => {
          val predictor = ErrorsPredictor()
          predictor.onDataset(dataset)
          val trainAndTestDFs: (DataFrame, DataFrame) = predictor.createTrainAndTestData(session)

          val trainDF: DataFrame = trainAndTestDFs._1
          val testDF: DataFrame = trainAndTestDFs._2

          var matrix: DataFrame = trainDF.union(testDF).toDF()
          matrix = matrix.drop("features")

          //matrix.printSchema()

          val path: String = pathToData.getOrElse(dataset, "")

          //                    matrix
          //                      .repartition(1)
          //                      .write
          //                      .option("header", "true")
          //                      .csv(path)


        })
      }
    }
  }
}