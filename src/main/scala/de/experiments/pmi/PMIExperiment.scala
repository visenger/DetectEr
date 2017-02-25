package de.experiments.pmi

import com.typesafe.config.ConfigFactory
import de.model.mutual.information.PMIEstimator

/**
  * Created by visenger on 25/02/17.
  */
class PMIExperiment {

}

trait PMIExperimentsCommonBase {
  val experiments = ConfigFactory.load("experiments.conf")

  def runMe(trainDataPath: String, testDataPath: String): Unit = {
    val estimator = new PMIEstimator()
    estimator.addTrainData(trainDataPath)
    estimator.addTestData(testDataPath)
    estimator.runPMIOnTrainAndTest()
  }
}

object HospPMIExperimentRunner extends PMIExperimentsCommonBase {
  def run() = {
    println("HOSP")

    val trainFile = "hosp.experiments.train.file"
    val testFile = "hosp.experiments.test.file"

    val trainDataPath = experiments.getString(trainFile)
    val testDataPath = experiments.getString(testFile)

    runMe(trainDataPath, testDataPath)
  }

  def main(args: Array[String]): Unit = {
    run()

  }
}

object BlackoakPMIExperimentRunner extends PMIExperimentsCommonBase {
  def run() = {
    println("BLACKOAK")
    val blackoakTrainFile = "blackoak.experiments.train.file"
    val blackoakTestFile = "blackoak.experiments.test.file"

    val trainDataPath = experiments.getString(blackoakTrainFile)
    val testDataPath = experiments.getString(blackoakTestFile)

    runMe(trainDataPath, testDataPath)

  }

  def main(args: Array[String]): Unit = {
    run()
  }
}

object SalariesPMIExperimentRunner extends PMIExperimentsCommonBase {
  def run() = {
    println("SALARIES")
    val salariesTrainFile = "salaries.experiments.train.file"
    val salariesTestFile = "salaries.experiments.test.file"

    val trainDataPath = experiments.getString(salariesTrainFile)
    val testDataPath = experiments.getString(salariesTestFile)

    runMe(trainDataPath, testDataPath)
  }

  def main(args: Array[String]): Unit = {
    run()
  }
}

object RunAllPMIExperiments {
  def main(args: Array[String]): Unit = {
    BlackoakPMIExperimentRunner.run()
    HospPMIExperimentRunner.run()
    SalariesPMIExperimentRunner.run()
  }
}
