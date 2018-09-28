package de.experiments

import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema._
import de.experiments.metadata._

/**
  * Common config variables for experiments.
  */
trait ExperimentsCommonConfig extends Serializable {

  val splitter = ","

  val defaultConfig = ConfigFactory.load()

  val experimentsConf = ConfigFactory.load("experiments.conf")

  val benchmarkConf = ConfigFactory.load("benchmark.conf")

  val blackoakTrainFile = experimentsConf.getString("blackoak.experiments.train.file")
  val hospTrainFile = experimentsConf.getString("hosp.experiments.train.file")
  val salariesTrainFile = experimentsConf.getString("salaries.experiments.train.file")
  val flightsTrainFile = experimentsConf.getString("flights.experiments.train.file")

  val blackoakTestFile = experimentsConf.getString("blackoak.experiments.test.file")
  val hospTestFile = experimentsConf.getString("hosp.experiments.test.file")
  val salariesTestFile = experimentsConf.getString("salaries.experiments.test.file")
  val flightsTestFile = experimentsConf.getString("flights.experiments.test.file")

  val allDBNames: Map[String, String] = Map(
    "blackoak" -> experimentsConf.getString("blackoak.db.name"),
    "hosp" -> experimentsConf.getString("hosp.db.name"),
    "salaries" -> experimentsConf.getString("salaries.db.name"),
    "flights" -> experimentsConf.getString("flights.db.name")
  )

  val allRepairedFiles: Map[String, String] = Map(
    "flights" -> experimentsConf.getString("flights.repaired.file")
  )

  val allNadeefRepairedFiles: Map[String, String] = Map(
    "flights" -> experimentsConf.getString("flights.nadeef.repair")
  )


  val allTestDataSets: Seq[String] = Seq(blackoakTestFile,
    hospTestFile,
    salariesTestFile,
    flightsTestFile)

  val allFDsDictionariesByName: Map[String, FDsDictionary] = Map(
    "blackoak" -> BlackOakFDsDictionary,
    "hosp" -> HospFDsDictionary,
    "salaries" -> SalariesFDsDictionary,
    "flights" -> FlightsFDsDictionary
  )

  val allSchemasByName: Map[String, Schema] = Map(
    "blackoak" -> BlackOakSchema,
    "hosp" -> HospSchema,
    "salaries" -> SalariesSchema,
    "flights" -> FlightsSchema,
    "beers" -> BeersSchema,

    "beers_dirty_5_explicitmissingvalue" -> BeersSchema,
    "beers_dirty_5_implicitmissingvaluemedianmode" -> BeersSchema,
    "beers_dirty_5_noise" -> BeersSchema,
    "beers_dirty_5_randomactivedomain" -> BeersSchema,
    "beers_dirty_5_similarbasedactivedomain" -> BeersSchema,
    "beers_dirty_5_typoGenerator" -> BeersSchema,

    "beers_missfielded_10" -> BeersSchema
  )

  val allMetadataByName: Map[String, String] = Map(
    "blackoak" -> defaultConfig.getString("metadata.blackoak.path"),
    "hosp" -> defaultConfig.getString("metadata.hosp.path"),
    "salaries" -> defaultConfig.getString("metadata.salaries.path"),
    "flights" -> defaultConfig.getString("metadata.flights.path"),
    "beers" -> defaultConfig.getString("metadata.beers.path"),

    "beers_missfielded_10" -> benchmarkConf.getString("metadata.beers.dirty_missfielded_10")
  )

  val allRawData: Map[String, String] = Map(
    "blackoak" -> defaultConfig.getString("data.blackoak.dirty"),
    //"blackoak" -> defaultConfig.getString("data.BlackOak.dirty-data-path"),
    "hosp" -> defaultConfig.getString("data.hosp.dirty.10k"),
    "salaries" -> defaultConfig.getString("data.salaries.dirty"),
    "flights" -> defaultConfig.getString("data.flights.dirty"),
    "beers" -> defaultConfig.getString("data.beers.dirty"),

    "beers_dirty_5_explicitmissingvalue" -> benchmarkConf.getString("data.beers.dirty_5_explicitmissingvalue"),
    "beers_dirty_5_implicitmissingvaluemedianmode" -> benchmarkConf.getString("data.beers.dirty_5_implicitmissingvaluemedianmode"),
    "beers_dirty_5_noise" -> benchmarkConf.getString("data.beers.dirty_5_noise"),
    "beers_dirty_5_randomactivedomain" -> benchmarkConf.getString("data.beers.dirty_5_randomactivedomain"),
    "beers_dirty_5_similarbasedactivedomain" -> benchmarkConf.getString("data.beers.dirty_5_similarbasedactivedomain"),
    "beers_dirty_5_typoGenerator" -> benchmarkConf.getString("data.beers.dirty_5_typoGenerator"),

    "beers_missfielded_10" -> benchmarkConf.getString("data.beers.dirty_missfielded_10")
  )

  val allCleanData: Map[String, String] = Map(
    "blackoak" -> defaultConfig.getString("data.blackoak.clean"),
    "flights" -> defaultConfig.getString("data.flights.clean"),
    "beers" -> defaultConfig.getString("data.beers.clean"),

    "beers_dirty_5_explicitmissingvalue" -> benchmarkConf.getString("data.beers.clean"),
    "beers_dirty_5_implicitmissingvaluemedianmode" -> benchmarkConf.getString("data.beers.clean"),
    "beers_dirty_5_noise" -> benchmarkConf.getString("data.beers.clean"),
    "beers_dirty_5_randomactivedomain" -> benchmarkConf.getString("data.beers.clean"),
    "beers_dirty_5_similarbasedactivedomain" -> benchmarkConf.getString("data.beers.clean"),
    "beers_dirty_5_typoGenerator" -> benchmarkConf.getString("data.beers.clean"),

    "beers_missfielded_10" -> benchmarkConf.getString("data.beers.clean")

  )

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

  def process_fds(f: Tuple2[String, FDsDictionary] => Unit): Unit = {
    allFDsDictionariesByName.foreach(data => f(data))
  }


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

case class UnknownPath(placeholder: String) {
  override def toString: String = placeholder
}
