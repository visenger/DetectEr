package de.experiments

import java.io.File

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.LibsvmConverter
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * Created by visenger on 24/02/17.
  */

trait SplitterCommonBase {
  val experimentsConf = ConfigFactory.load("experiments.conf")
  val trainFraction = experimentsConf.getDouble("train.fraction")
  val testFraction = experimentsConf.getDouble("test.fraction")


  val defaultConfig = ConfigFactory.load()

  def writeTextTo(data: DataFrame, folder: String): Unit = {
    val path = experimentsConf.getString(folder)
    data
      .coalesce(1)
      .write
      .text(path)
  }

  def writeCSV(dataFrame: DataFrame, folder: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", true)
      .save(s"${experimentsConf.getString(folder)}")
  }

}

class DataSplitter extends SplitterCommonBase {

  private var datasetFullResult = ""

  private var datasetTrain = ""
  private var datasetTrainLibsvm = ""
  private var datasetTrainFile = ""
  private var datasetTrainLibsvmFile = ""

  private var datasetTest = ""
  private var datasetTestLibsvm = ""
  private var datasetTestFile = ""
  private var datasetTestLibsvmFile = ""

  def takeFullResult(full: String): this.type = {
    datasetFullResult = full
    this
  }

  def addTrainFolder(train: String): this.type = {
    datasetTrain = train
    this
  }

  def addTrainLibsvmFolder(libsvm: String): this.type = {
    datasetTrainLibsvm = libsvm
    this
  }

  def addTestFolder(test: String): this.type = {
    datasetTest = test
    this
  }

  def addTestLibsvmFolder(libsvm: String): this.type = {
    datasetTestLibsvm = libsvm
    this
  }

  def renameTrainTo(file: String): this.type = {
    datasetTrainFile = file
    this
  }

  def renameTrainLibsvmTo(file: String): this.type = {
    datasetTrainLibsvmFile = file
    this
  }

  def renameTestTo(file: String): this.type = {
    datasetTestFile = file
    this
  }

  def renameTestLibsvmTo(file: String): this.type = {
    datasetTestLibsvmFile = file
    this
  }

  def run() = {
    SparkLOAN.withSparkSession("SPLIT") {
      session => {
        import session.implicits._
        val data = DataSetCreator.createDataSetFromCSV(session, datasetFullResult, FullResult.schema: _*)
        val Array(train, test) = data.randomSplit(Array(trainFraction, testFraction))

        val libsvmConverter = new LibsvmConverter()

        val trainLibsvm: DataFrame = libsvmConverter.toLibsvmFormat(train)

        val testLibsvm: DataFrame = libsvmConverter.toLibsvmFormat(test)

        train.show()

        testLibsvm.show()

        val trainDF = train.toDF(FullResult.schema: _*)
        writeCSV(trainDF, datasetTrain)
        writeTextTo(trainLibsvm, datasetTrainLibsvm)

        val testDF = test.toDF(FullResult.schema: _*)
        writeCSV(testDF, datasetTest)
        writeTextTo(testLibsvm, datasetTestLibsvm)

      }
    }
  }

  def renameFile(path: String): Unit = {
    val filePath = experimentsConf.getString(path)
    // experiments.train.file = ${home.dir.blackoak}"/experiments/split/train/full-result-blackoak-train.csv"
    val idx = filePath.lastIndexOf("/")
    val fileName = filePath.substring(idx + 1)

    val folderName = filePath.substring(0, idx)
    println(s"file:  $fileName in folder: $folderName")

    // mv(s"$folderName/part*.csv", filePath)
    import sys.process._
    //s"""mv $folderName/part* $filePath""" !

    Seq("/bin/sh", "-c", s"mv $folderName/part* $filePath").!
    //Seq("/bin/sh", "-c", s"rm -f $folderName/.* ").!
    Seq("/bin/sh", "-c", s"rm -f $folderName/_* ").!

  }

  private def mv(oldName: String, newName: String): Unit = {
    val file = new File(oldName)
    println(file.getName)
    Try(file.renameTo(new File(newName))).getOrElse(false)
  }


}

object BlackOakDataSplitter {

  val blackoakFullResult = "output.full.result.file"

  val blackoakTrain = "blackoak.experiments.train.folder"
  val blackoakTrainLibsvm = "blackoak.experiments.train.libsvm.folder"
  val blackoakTrainFile = "blackoak.experiments.train.file"
  val blackoakTrainLibsvmFile = "blackoak.experiments.train.libsvm.file"

  val blackoakTest = "blackoak.experiments.test.folder"
  val blackoakTestLibsvm = "blackoak.experiments.test.libsvm.folder"
  val blackoakTestFile = "blackoak.experiments.test.file"
  val blackoakTestLibsvmFile = "blackoak.experiments.test.libsvm.file"

  val allNewFiles = Seq(blackoakTrainFile,
    blackoakTrainLibsvmFile,
    blackoakTestFile,
    blackoakTestLibsvmFile)

  def main(args: Array[String]): Unit = {
    val splitter = new DataSplitter()
    splitter.takeFullResult(blackoakFullResult)

    splitter.addTrainFolder(blackoakTrain)
    splitter.addTrainLibsvmFolder(blackoakTrainLibsvm)

    splitter.addTestFolder(blackoakTest)
    splitter.addTestLibsvmFolder(blackoakTestLibsvm)

    splitter.run()

    allNewFiles.foreach(file => splitter.renameFile(file))

  }
}

object HospDataSplitter {
  val hospFullResult = "result.hosp.10k.full.result.file "

  val hospTrain = "hosp.experiments.train.folder"
  val hospTrainLibsvm = "hosp.experiments.train.libsvm.folder"
  val hospTrainFile = "hosp.experiments.train.file"
  val hospTrainLibsvmFile = "hosp.experiments.train.libsvm.file"

  val hospTest = "hosp.experiments.test.folder"
  val hospTestLibsvm = "hosp.experiments.test.libsvm.folder"
  val hospTestFile = "hosp.experiments.test.file"
  val hospTestLibsvmFile = "hosp.experiments.test.libsvm.file"

  val allNewFiles = Seq(hospTrainFile,
    hospTrainLibsvmFile,
    hospTestFile,
    hospTestLibsvmFile)

  def main(args: Array[String]): Unit = {

    val dataSplitter = new DataSplitter()
    dataSplitter.takeFullResult(hospFullResult)

    dataSplitter.addTrainFolder(hospTrain)
    dataSplitter.addTrainLibsvmFolder(hospTrainLibsvm)

    dataSplitter.addTestFolder(hospTest)
    dataSplitter.addTestLibsvmFolder(hospTestLibsvm)

    dataSplitter.run()

    allNewFiles.foreach(file => dataSplitter.renameFile(file))

  }
}

object SalariesDataSplitter {
  val salariesFullResult = "result.salaries.full.result.file"

  val salariesTrain = "salaries.experiments.train.folder"
  val salariesTrainLibsvm = "salaries.experiments.train.libsvm.folder"
  val salariesTrainFile = "salaries.experiments.train.file"
  val salariesTrainLibsvmFile = "salaries.experiments.train.libsvm.file"

  val salariesTest = "salaries.experiments.test.folder"
  val salariesTestLibsvm = "salaries.experiments.test.libsvm.folder"
  val salariesTestFile = "salaries.experiments.test.file"
  val salariesTestLibsvmFile = "salaries.experiments.test.libsvm.file"

  val allNewFiles = Seq(salariesTrainFile,
    salariesTrainLibsvmFile,
    salariesTestFile,
    salariesTestLibsvmFile)

  def main(args: Array[String]): Unit = {

    val dataSplitter = new DataSplitter()
    dataSplitter.takeFullResult(salariesFullResult)

    dataSplitter.addTrainFolder(salariesTrain)
    dataSplitter.addTrainLibsvmFolder(salariesTrainLibsvm)

    dataSplitter.addTestFolder(salariesTest)
    dataSplitter.addTestLibsvmFolder(salariesTestLibsvm)

    dataSplitter.run()

    allNewFiles.foreach(file => dataSplitter.renameFile(file))


  }
}

