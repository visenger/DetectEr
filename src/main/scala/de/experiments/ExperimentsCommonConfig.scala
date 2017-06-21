package de.experiments

import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema._

/**
  * Common config variables for experiments.
  */
trait ExperimentsCommonConfig {

  val splitter = ","

  val defaultConfig = ConfigFactory.load()

  val experimentsConf = ConfigFactory.load("experiments.conf")

  val blackoakTrainFile = experimentsConf.getString("blackoak.experiments.train.file")
  val hospTrainFile = experimentsConf.getString("hosp.experiments.train.file")
  val salariesTrainFile = experimentsConf.getString("salaries.experiments.train.file")
  val flightsTrainFile = experimentsConf.getString("flights.experiments.train.file")

  val blackoakTestFile = experimentsConf.getString("blackoak.experiments.test.file")
  val hospTestFile = experimentsConf.getString("hosp.experiments.test.file")
  val salariesTestFile = experimentsConf.getString("salaries.experiments.test.file")
  val flightsTestFile = experimentsConf.getString("flights.experiments.test.file")


  val allTestDataSets: Seq[String] = Seq(blackoakTestFile,
    hospTestFile,
    salariesTestFile,
    flightsTestFile)

  val allSchemasByName: Map[String, Schema] = Map(
    "blackoak" -> BlackOakSchema,
    "hosp" -> HospSchema,
    "salaries" -> SalariesSchema,
    "flights" -> FlightsSchema
  )

  val allMetadataByName: Map[String, String] = Map(
    "blackoak" -> defaultConfig.getString("metadata.blackoak.path"),
    "hosp" -> defaultConfig.getString("metadata.hosp.path"),
    "salaries" -> defaultConfig.getString("metadata.salaries.path"),
    "flights" -> defaultConfig.getString("metadata.flights.path")
  )

  val allRawData: Map[String, String] = Map(
    "blackoak" -> defaultConfig.getString("data.BlackOak.dirty-data-path"),
    "hosp" -> defaultConfig.getString("data.hosp.dirty.10k"),
    "salaries" -> defaultConfig.getString("data.salaries.dirty"),
    "flights" -> defaultConfig.getString("data.flights.dirty"))

  val allTestData: Map[String, String] = Map(
    "blackoak" -> blackoakTestFile,
    "hosp" -> hospTestFile,
    "salaries" -> salariesTestFile,
    "flights" -> flightsTestFile)

  val allTrainData: Map[String, String] = Map(
    "blackoak" -> blackoakTrainFile,
    "hosp" -> hospTrainFile,
    "salaries" -> salariesTrainFile,
    "flights" -> flightsTrainFile
  )

  val allTrainAndTestData: Map[String, (String, String)] = Map(

    "blackoak" -> (blackoakTrainFile, blackoakTestFile),
    "hosp" -> (hospTrainFile, hospTestFile),
    "salaries" -> (salariesTrainFile, salariesTestFile),
    "flights" -> (flightsTrainFile, flightsTestFile)
  )


  def process_test_data(f: Tuple2[String, String] => Unit): Unit = {
    allTestData.foreach(data => f(data))
  }

  def process_train_data(f: Tuple2[String, String] => Unit): Unit = {
    allTrainData.foreach(data => f(data))
  }

  def process_data(f: Tuple2[String, (String, String)] => Unit): Unit = {
    allTrainAndTestData.foreach(data => f(data))
  }

  def getName(tool: String) = experimentsConf.getString(s"dictionary.names.$tool")

  /** EXPERIMENTS WITH EXTERNAL DATA */

  val extBlackoakTrainFile = experimentsConf.getString("ext.blackoak.experiments.train.file")
  val extBlackoakTestFile = experimentsConf.getString("ext.blackoak.experiments.test.file")

  val allExternalData: Map[String, (String, String)] =
    Map("ext.blackoak" -> (extBlackoakTrainFile, extBlackoakTestFile))

  def process_ext_data(f: Tuple2[String, (String, String)] => Unit): Unit = {
    allExternalData.foreach(data => f(data))
  }

  def getExtName(tool: String) = experimentsConf.getString(s"ext.dictionary.names.$tool")


}
