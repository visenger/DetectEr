package de.evaluation.tools.outliers.dboost

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.f1.Eval
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


@Deprecated
object EvaluatorDBoost {

  def main(args: Array[String]): Unit = {

    val cleanData = "data.BlackOak.clean-data-path"
    val dirtyData = "data.BlackOak.dirty-data-path"

    val config: Config = ConfigFactory.load()

    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("F1")
      .config("spark.local.ip",
        config.getString("spark.config.local.ip.value"))
      .config("spark.driver.memory", "10g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()

    val schema = Seq("RecID", "FirstName", "MiddleName", "LastName", "Address", "City", "State", "ZIP", "POBox", "POCityStateZip", "SSN", "DOB")


    val dirtyBlackOakDF: DataFrame = createDataSet(sparkSession, dirtyData, schema: _*)
    //dirtyBlackOakDF.createOrReplaceTempView("dirtyDB")
    //dirtyBlackOakDF.show(20)


    val cleanBlackOakDF: DataFrame = createDataSet(sparkSession, cleanData, schema: _*)

    // cleanBlackOakDF.show(20)

    //cleanBlackOakDF.createOrReplaceTempView("cleanDB")


    val goldStandard: Dataset[String] = getGoldStandard(sparkSession, dirtyBlackOakDF, cleanBlackOakDF)
    //goldStandard.show(12)

    //    val pathHist = ConfigFactory.load().getString("dboost.BlackOak.result.hist")
    //    val outliers: Dataset[String] = getOutliers(sparkSession, pathHist)
    //
    //    val pathGauss = ConfigFactory.load().getString("dboost.BlackOak.result.gaus")
    //    val outliersG: Dataset[String] = getOutliers(sparkSession, pathGauss)
    //
    //    /* compute F1 measure*/
    //    val resultH = evaluateResult(goldStandard, outliers)
    //    val resultG: Eval = evaluateResult(goldStandard, outliersG)
    //
    //    println(s"Result dBoost hist 0.9 0.01: precision= ${resultH.precision} , recall = ${resultH.recall} , F1= ${resultH.f1} ")
    //    println(s"Result dBoost gaus 3: precision = ${resultG.precision}, recall = ${resultG.recall}, F1= ${resultG.f1}")

    sparkSession.stop()
  }

  def evaluateResult(goldStandard: Dataset[String], selected: Dataset[String]): Eval = {
    val tpDataset: Dataset[String] = goldStandard.intersect(selected)
    val tp: Long = tpDataset.count()

    val fnDataset: Dataset[String] = goldStandard.except(tpDataset)
    val fn = fnDataset.count()

    val fpDataset: Dataset[String] = selected.except(tpDataset)
    val fp: Long = fpDataset.count()

    //println(s"tp= $tp, fn= $fn, fp=$fp")
    val precision = tp.toDouble / (tp + fp).toDouble
    val recall = tp.toDouble / (tp + fn).toDouble
    val F1 = (2 * precision * recall) / (precision + recall)
    Eval(precision, recall, F1)
  }

  def getGoldStandard(sparkSession: SparkSession, dirtyBlackOakDF: DataFrame, cleanBlackOakDF: DataFrame): Dataset[String] = {
    import sparkSession.implicits._
    val join = cleanBlackOakDF
      .join(dirtyBlackOakDF, cleanBlackOakDF.col("RecID") === dirtyBlackOakDF.col("RecID"))
      .filter(
        cleanBlackOakDF.col("FirstName") =!= dirtyBlackOakDF.col("FirstName")
          || cleanBlackOakDF.col("MiddleName") =!= dirtyBlackOakDF.col("MiddleName")
          || cleanBlackOakDF.col("LastName") =!= dirtyBlackOakDF.col("LastName")
          || cleanBlackOakDF.col("Address") =!= dirtyBlackOakDF.col("Address")
          || cleanBlackOakDF.col("City") =!= dirtyBlackOakDF.col("City")
          || cleanBlackOakDF.col("State") =!= dirtyBlackOakDF.col("State")
          || cleanBlackOakDF.col("ZIP") =!= dirtyBlackOakDF.col("ZIP")
          || cleanBlackOakDF.col("POBox") =!= dirtyBlackOakDF.col("POBox")
          || cleanBlackOakDF.col("POCityStateZip") =!= dirtyBlackOakDF.col("POCityStateZip")
          || cleanBlackOakDF.col("SSN") =!= dirtyBlackOakDF.col("SSN")
          || cleanBlackOakDF.col("DOB") =!= dirtyBlackOakDF.col("DOB"))

    val columns = join.columns
    columns.foreach(println)
    val dtypes = join.dtypes
    dtypes.foreach(println)
    //join.show(50)

    val goldStd: DataFrame = join.select(cleanBlackOakDF.col("RecID"))
    val goldStandard: Dataset[String] = goldStd.map(row => row.getAs[String](0).trim)
    goldStandard
  }

  def getOutliers(sparkSession: SparkSession, file: String): Dataset[String] = {
    import sparkSession.implicits._
    val result: DataFrame = sparkSession.read.text(file)
    val firstTwoRows: Array[Row] = result.head(2)
    val filterHead: DataFrame = result.filter(row => !firstTwoRows.contains(row))
    val outliers: Dataset[String] = filterHead.map(row => row.getAs[String](0).trim.split("\\s").head)

    outliers
  }

  def createDataSet(sparkSession: SparkSession, dataPathStr: String, schema: String*): DataFrame = {

    val config: Config = ConfigFactory.load()
    val dataPath: String = config.getString(dataPathStr)

    val df: DataFrame = sparkSession.read.csv(dataPath)
    val namedDF: DataFrame = df.toDF(schema: _*)

    val head: Row = namedDF.head()
    val data: DataFrame = namedDF.filter(row => row != head).toDF()
    data
  }
}
