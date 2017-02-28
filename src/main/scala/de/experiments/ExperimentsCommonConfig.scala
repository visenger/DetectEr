package de.experiments

import com.typesafe.config.ConfigFactory

/**
  * Common config variables for experiments.
  */
trait ExperimentsCommonConfig {

  val experimentsConfig = ConfigFactory.load("experiments.conf")

  val blackoakTrainFile = experimentsConfig.getString("blackoak.experiments.train.file")
  val hospTrainFile = experimentsConfig.getString("hosp.experiments.train.file")
  val salariesTrainFile = experimentsConfig.getString("salaries.experiments.train.file")

  val blackoakTestFile = experimentsConfig.getString("blackoak.experiments.test.file")
  val hospTestFile = experimentsConfig.getString("hosp.experiments.test.file")
  val salariesTestFile = experimentsConfig.getString("salaries.experiments.test.file")

  val allTestDataSets: Seq[String] = Seq(blackoakTestFile, hospTestFile, salariesTestFile)

  def process_data(f: String => Unit): Unit = {
    allTestDataSets.foreach(data => f(data))
  }


}
