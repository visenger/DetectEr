package de.experiments

import com.typesafe.config.ConfigFactory

/**
  * Created by visenger on 24/02/17.
  */

trait SplitterCommonBase {
  val experimentsConf = ConfigFactory.load("experiments.conf")

  val defaultConfig = ConfigFactory.load()
  val hospFullResult = "result.hosp.10k.full.result.file"
  val salariesFullResult = "result.salaries.full.result.file"
  val blackoakFullResult = "output.full.result.file"
}

class DataSplitter {

}

object DataSplitterRunner extends SplitterCommonBase {
  def main(args: Array[String]): Unit = {

    println(experimentsConf.getDouble("test.fraction"))

    println(defaultConfig.getString(hospFullResult))


  }
}


