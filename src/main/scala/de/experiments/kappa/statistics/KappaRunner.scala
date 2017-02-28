package de.experiments.kappa.statistics

import de.experiments.ExperimentsCommonConfig
import de.model.kappa.KappaEstimator

/**
  * Created by visenger on 28/02/17.
  */
object BlackOakKappaRunner extends ExperimentsCommonConfig {

  def run() = {
    println(s"RUNNING BLACKOAK")
    val kappaEstimator = new KappaEstimator()
    kappaEstimator.trainKappaOnData(blackoakTrainFile)
    kappaEstimator.testKappaOnData(blackoakTestFile)
    kappaEstimator.runKappa()
  }

  def main(args: Array[String]): Unit = {
    run()
  }

}

object HospKappaRunner extends ExperimentsCommonConfig {
  def run() = {
    println(s"RUNNING HOSP")
    val kappaEstimator = new KappaEstimator()
    kappaEstimator.trainKappaOnData(hospTrainFile)
    kappaEstimator.testKappaOnData(hospTestFile)
    kappaEstimator.runKappa()
  }
}

object SalariesKappaRunner extends ExperimentsCommonConfig {
  def run() = {
    println(s"RUNNING SALARIES")
    val kappaEstimator = new KappaEstimator()
    kappaEstimator.trainKappaOnData(salariesTrainFile)
    kappaEstimator.testKappaOnData(salariesTestFile)
    kappaEstimator.runKappa()
  }
}

object KappaRunnerAll {
  def main(args: Array[String]): Unit = {
    BlackOakKappaRunner.run()
    HospKappaRunner.run()
    SalariesKappaRunner.run()
  }
}
