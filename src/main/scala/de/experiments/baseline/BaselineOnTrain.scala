package de.experiments.baseline

import de.experiments.ExperimentsCommonConfig
import de.model.baseline.Baseline

/**
  * Created by visenger on 27/02/17.
  */


class BaselineOnTrain extends ExperimentsCommonConfig {


}

object BlackoakBaselineOnTrain extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    println("BLACKOAK")
    run()
  }

  def run() = {
    val baseline = new Baseline()
    baseline.onData(blackoakTrainFile)
    baseline.calculateEvalForEachTool()
    baseline.calculateBaseline()
  }
}

object HospBaselineOnTrain extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    println("HOSP")
    run()
  }

  def run() = {
    val baseline = new Baseline()
    baseline.onData(hospTrainFile)
    baseline.calculateEvalForEachTool()
    baseline.calculateBaseline()
  }
}

object FlightsBaselingOnTrain extends ExperimentsCommonConfig {

  def main(args: Array[String]): Unit = {
    println("FLIGHTS:")
    run()
  }

  def run() = {
    val baseline = new Baseline()
    baseline.onData(flightsTrainFile)
    baseline.calculateEvalForEachTool()
    baseline.calculateBaseline()
  }

}

object SalariesBaselineOnTrain extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    println("SALARIES:")
    run()
  }

  def run() = {
    val baseline = new Baseline()
    baseline.onData(salariesTrainFile)
    baseline.calculateEvalForEachTool()
    baseline.calculateBaseline()
  }
}

object FlightsBaselineOnTrain extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    println("FLIGHTS:")
    run()
  }

  def run() = {
    val baseline = new Baseline()
    baseline.onData(flightsTrainFile)
    baseline.calculateEvalForEachTool()
    baseline.calculateBaseline()
  }
}

object BaselineOnTrainRunAll {
  def main(args: Array[String]): Unit = {
    BlackoakBaselineOnTrain.run()
    HospBaselineOnTrain.run()
    SalariesBaselineOnTrain.run()
    FlightsBaselineOnTrain.run()
  }
}
