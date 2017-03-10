package de.experiments

import com.typesafe.config.ConfigFactory

/**
  * Common config variables for experiments.
  */
trait ExperimentsCommonConfig {

  val experimentsConf = ConfigFactory.load("experiments.conf")

  val blackoakTrainFile = experimentsConf.getString("blackoak.experiments.train.file")
  val hospTrainFile = experimentsConf.getString("hosp.experiments.train.file")
  val salariesTrainFile = experimentsConf.getString("salaries.experiments.train.file")

  val blackoakTestFile = experimentsConf.getString("blackoak.experiments.test.file")
  val hospTestFile = experimentsConf.getString("hosp.experiments.test.file")
  val salariesTestFile = experimentsConf.getString("salaries.experiments.test.file")

  val allTestDataSets: Seq[String] = Seq(blackoakTestFile, hospTestFile, salariesTestFile)


  val allTestData: Map[String, String] = Map("blackoak" -> blackoakTestFile,
    "hosp" -> hospTestFile,
    "salaries" -> salariesTestFile)

  val allTrainData: Map[String, String] = Map("blackoak" -> blackoakTrainFile,
    "hosp" -> hospTrainFile,
    "salaries" -> salariesTrainFile)


  def process_test_data(f: Tuple2[String, String] => Unit): Unit = {
    allTestData.foreach(data => f(data))
  }

  def process_train_data(f: Tuple2[String, String] => Unit): Unit = {
    allTrainData.foreach(data => f(data))
  }

  def getName(tool: String) = experimentsConf.getString(s"dictionary.names.$tool")


}
