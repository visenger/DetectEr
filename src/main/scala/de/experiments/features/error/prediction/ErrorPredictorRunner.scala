package de.experiments.features.error.prediction

import de.evaluation.util.SparkLOAN

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
