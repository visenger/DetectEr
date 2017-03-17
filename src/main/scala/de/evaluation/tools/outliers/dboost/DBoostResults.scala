package de.evaluation.tools.outliers.dboost

import java.io.File

import com.google.common.base.Strings
import com.typesafe.config.ConfigFactory
import de.evaluation.data.schema.{BlackOakSchema, HospSchema, SalariesSchema, Schema}
import de.evaluation.f1.Cells
import de.evaluation.util.{DataSetCreator, SparkLOAN, SparkSessionCreator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}


class DBoostResults {


  private var dboostDetectFile = ""
  private var outputPath = ""

  private var schema: Schema = null
  private var recid = ""

  val conf = ConfigFactory.load()

  def onSchema(s: Schema): this.type = {
    schema = s
    recid = s.getRecID
    this
  }

  def addDetectFile(f: String): this.type = {
    dboostDetectFile = f
    this
  }

  def addOutputFolder(out: String): this.type = {
    outputPath = out
    this
  }

  def showOutliersLog(numRows: Int = 20): Unit = {

    SparkLOAN.withSparkSession("DBOOST") {
      session => {
        val result = DataSetCreator.createDataSetNoHeader(session, dboostDetectFile, schema.getSchema: _*)
        val outliersLog: Dataset[String] = createOutliersLog(session, result)
        outliersLog.show(numRows)
      }
    }

  }

  def writeOutliersLog(): Unit = {

    SparkLOAN.withSparkSession("DBOOST") {
      session => {
        val result = DataSetCreator.createDataSetNoHeader(session, dboostDetectFile, schema.getSchema: _*)
        val outliersLog: Dataset[String] = createOutliersLog(session, result)
        outliersLog.show(123)
        outliersLog
          .coalesce(1)
          .write
          .text(conf.getString(outputPath))
      }
    }

  }

  def getGaussResultForOutlierDetection(sparkSession: SparkSession): DataFrame = {

    val path = "output.dboost.gaus.result.file"

    val gausResults: DataFrame = getOutliersByAlgorithm(sparkSession, path)
    gausResults
  }

  def getHistogramResultForOutlierDetection(sparkSession: SparkSession): DataFrame = {

    val path = "output.dboost.result.file"

    val histResults: DataFrame = getOutliersByAlgorithm(sparkSession, path)
    histResults
  }

  def getOutliersByAlgorithm(sparkSession: SparkSession, path: String) = {
    val resultDataSet: DataFrame = DataSetCreator
      .createDataSet(
        sparkSession,
        path,
        Cells.schema: _*)

    resultDataSet
  }

  @Deprecated
  def writeResultsForMultipleFiles() = {

    val outliersResults = conf.getString("dboost.small.output.folder")
    val evalOutputFolder = conf.getString("dboost.small.eval.folder")

    val dir = new File(outliersResults)
    val filesList: List[File] = getOutliersFiles(dir)

    val session = SparkSessionCreator.createSession("DBOOST-SMALL")

    //todo: 1. get all dboost small results -> parametrisation

    filesList.foreach(file => {
      val filePath = file.getAbsolutePath
      val dirName = file.getName.substring(0, file.getName.lastIndexOf("."))
      // todo: hack! sometimes we get the following exception:
      /*
      * java.lang.IllegalArgumentException: requirement failed: The number of columns doesn't match.
        Old column names (13): _c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7, _c8, _c9, _c10, _c11, _c12
        New column names (12): RecID, FirstName, MiddleName, LastName, Address, City, State, ZIP, POBox, POCityStateZip, SSN, DOB
      *
      * */
      //todo: in case of Failure we can try to remove some couple of lines and try again
      val tried = Try(writeResultForFile(session, filePath, s"$evalOutputFolder/$dirName"))
      tried match {
        case Success(_) => println(s" OK $dirName")
        case Failure(f) => println(f)
      }
    })


    session.stop()
  }

  private def getOutliersFiles(dir: File): List[File] = {
    dir.listFiles().filter(_.isFile).toList
  }

  private def writeResultForFile(session: SparkSession, file: String, outputFolder: String): Unit = {

    val resultDataSet: DataFrame = DataSetCreator
      .createFrameNoHeader(
        session,
        file,
        BlackOakSchema.schema: _*)

    val outliersDS: Dataset[String] = createOutliersLog(session, resultDataSet)

    //todo: write results to folder
    outliersDS.write.text(outputFolder)

  }

  private def createOutliersLog(sparkSession: SparkSession, resultDataSet: DataFrame): Dataset[String] = {
    //every attribute marked as ~attr~ is being identified as outlier
    import sparkSession.implicits._

    val thisSchema: Schema = schema
    val id = thisSchema.getRecID
    val allAttrNames = thisSchema.getSchema
    val oulierMark = "~"

    //here we create all parts of condition
    val allColumns: Seq[Column] = allAttrNames
      .filterNot(_.equals(id))
      .map(name => resultDataSet.col(name).contains(oulierMark))


    val condition: Column = allColumns.tail
      .foldLeft(allColumns.head)((acc: Column, actual: Column) => acc || actual)

    val filter = resultDataSet.filter(condition)

    val outliers: RDD[String] = filter.rdd.flatMap(row => {
      val vals: Map[String, String] = row.getValuesMap[String](allAttrNames)
      val filtered = vals.filter(a =>
        !a._1.equalsIgnoreCase(id) //just in case when id is considered as outlier, we have to exclude it
          && !Strings.isNullOrEmpty(a._2)
          && a._2.contains(oulierMark))
      val idx: List[Int] = thisSchema.getIndexesByAttrNames(filtered.keySet.toList)

      val recID: String = row.getAs[String](id).trim

      //TODO: sometimes dboost takes recid as outliers. Here is a temp fix.
      val scapedRecId = recID.contains(oulierMark) match {
        case true => {
          recID.replaceAll(oulierMark, "").trim
        }
        case false => recID
      }

      val outliersLines: List[String] = idx.map(i => s"$scapedRecId,$i")
      outliersLines
    })

    val outliersDS: Dataset[String] = outliers.filter(!_.isEmpty).toDS()
    outliersDS
  }


}

object DBoostResults {
  /*def main(args: Array[String]): Unit = {
    new DBoostResults().createOutliersLog()

    //new DBoostResults().writeResults()
    //new DBoostResultWriter().writeResultsForMultipleFiles()
  }
*/
  def getHistogramResultForOutlierDetection(session: SparkSession): DataFrame = {
    new DBoostResults().getHistogramResultForOutlierDetection(session)
  }

  def getGaussResultForOutlierDetection(session: SparkSession): DataFrame = {
    new DBoostResults().getGaussResultForOutlierDetection(session)
  }

}

object HospHistDBoostResults {
  val oulierDetectFile = "dboost.hosp.detect.hist"
  val folder = "dboost.hosp.result.hist"

  def main(args: Array[String]): Unit = {
    val dboost = new DBoostResults()

    dboost.onSchema(HospSchema)
    dboost.addDetectFile(oulierDetectFile)
    dboost.addOutputFolder(folder)
    dboost.writeOutliersLog()

  }

  def getResults(session: SparkSession): DataFrame = {
    val hospOutput = "result.hosp.10k.outlier.hist"
    new DBoostResults().getOutliersByAlgorithm(session, hospOutput)
  }
}

object HospGaussDBoostResults {
  val oulierDetectFile = "dboost.hosp.detect.gauss"
  val folder = "dboost.hosp.result.gauss"

  def main(args: Array[String]): Unit = {
    val dboost = new DBoostResults()

    dboost.onSchema(HospSchema)
    dboost.addDetectFile(oulierDetectFile)
    dboost.addOutputFolder(folder)
    dboost.writeOutliersLog()
  }

  def getResults(session: SparkSession): DataFrame = {
    val hospOutput = "result.hosp.10k.outlier.gauss"
    new DBoostResults().getOutliersByAlgorithm(session, hospOutput)
  }
}

object SalariesHistDBoostResults {
  val oulierDetectFile = "dboost.salaries.detect.hist"
  val folder = "dboost.salaries.result.hist"

  def main(args: Array[String]): Unit = {
    val dboost = new DBoostResults()

    dboost.onSchema(SalariesSchema)
    dboost.addDetectFile(oulierDetectFile)
    dboost.addOutputFolder(folder)
    dboost.writeOutliersLog()
  }

  def getResults(session: SparkSession): DataFrame = {
    val hospOutput = "result.salaries.outlier.hist"
    new DBoostResults().getOutliersByAlgorithm(session, hospOutput)
  }

}

object SalariesGaussDBoostResults {
  val oulierDetectFile = "dboost.salaries.detect.gauss"
  val folder = "dboost.salaries.result.gauss"

  def main(args: Array[String]): Unit = {
    val dboost = new DBoostResults()

    dboost.onSchema(SalariesSchema)
    dboost.addDetectFile(oulierDetectFile)
    dboost.addOutputFolder(folder)
    dboost.writeOutliersLog()
  }

  def getResults(session: SparkSession): DataFrame = {
    val hospOutput = "result.salaries.outlier.gauss"
    new DBoostResults().getOutliersByAlgorithm(session, hospOutput)
  }

}

object BlackOakGaussDBoostResults {
  val outlierDetectFile = "dboost.BlackOak.result.gaus"

  def main(args: Array[String]): Unit = {
    val dboost = new DBoostResults()

    dboost.onSchema(BlackOakSchema)
    dboost.addDetectFile(outlierDetectFile)
    dboost.addOutputFolder("")
    dboost.showOutliersLog()
  }

  def getResults(session: SparkSession): DataFrame = {
    val hospOutput = "output.dboost.gaus.result.file"
    new DBoostResults().getOutliersByAlgorithm(session, hospOutput)
  }
}

object BlackOakHistDBoostResults {
  val outlierDetectFile = "dboost.BlackOak.result.dir"

  def main(args: Array[String]): Unit = {
    val dboost = new DBoostResults()

    dboost.onSchema(BlackOakSchema)
    dboost.addDetectFile(outlierDetectFile)
    dboost.addOutputFolder("")
    dboost.showOutliersLog()
  }
}
