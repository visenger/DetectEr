package de.evaluation.tools.outliers.dboost

import java.io.File

import com.google.common.base.Strings
import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.blackoak.BlackOakSchema
import de.evaluation.util.{DataSetCreator, SparkSessionCreator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}


class DBoostResultWriter {


  val conf: Config = ConfigFactory.load()

  def writeResults(): Unit = {
    val sparkSession: SparkSession = SparkSessionCreator.createSession("DBOOST")

    val path = "dboost.BlackOak.result.gaus"
    val outputFolder = "output.dboost.gaus.result.folder"

    val resultDataSet: DataFrame = DataSetCreator
      .createDataSet(
        sparkSession,
        path,
        BlackOakSchema.schema: _*)

    val outliersDS: Dataset[String] = createOutliersLog(sparkSession, resultDataSet)

    //todo: write results to folder
    outliersDS.write.text(conf.getString(outputFolder))

    sparkSession.stop()

  }

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
      .createDataSetFromFileNoHeader(
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

    val allColumns: Seq[Column] = BlackOakSchema.schema.map(name => resultDataSet.col(name).contains("~"))

    val condition: Column = allColumns.tail.foldLeft(allColumns.head)((acc: Column, actual: Column) => acc || actual)

    val filter = resultDataSet.filter(condition)

    val outliers: RDD[String] = filter.rdd.flatMap(row => {
      val vals: Map[String, String] = row.getValuesMap[String](BlackOakSchema.schema)
      val filtered = vals.filter(a => !Strings.isNullOrEmpty(a._2) && a._2.contains("~"))
      val idx: List[Int] = BlackOakSchema.getIndexesByAttrNames(filtered.keySet.toList)
      val recID: String = row.getAs[String]("RecID").trim
      val outliersLines: List[String] = idx.map(i => s"$recID,$i")
      outliersLines
    })

    val outliersDS: Dataset[String] = outliers.filter(!_.isEmpty).toDS()
    outliersDS
  }
}

object DBoostResultWriter {
  def main(args: Array[String]): Unit = {
    //  new DBoostResultWriter().writeResults()
    new DBoostResultWriter().writeResultsForMultipleFiles()
  }

}
