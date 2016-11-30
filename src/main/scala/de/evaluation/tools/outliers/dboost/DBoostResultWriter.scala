package de.evaluation.tools.outliers.dboost

import com.google.common.base.Strings
import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.blackoak.BlackOakSchema
import de.evaluation.util.{DataSetCreator, SparkSessionCreator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by visenger on 29/11/16.
  */
class DBoostResultWriter {

  val conf: Config = ConfigFactory.load()

  def writeResults(): Unit = {
    val sparkSession: SparkSession = SparkSessionCreator.createSession("DBOOST")
    //createSession()
    val path = "dboost.BlackOak.result.gaus"
    val outputFolder = "output.dboost.gaus.result.folder"

    val resultDataSet: DataFrame = DataSetCreator
      .createDataSet(
        sparkSession,
        path,
        BlackOakSchema.schema: _*)

    resultDataSet.show(5)

    //every attribute marked as ~attr~ is being identified as outlier
    import sparkSession.implicits._

    val allColumns: Seq[Column] = BlackOakSchema.schema.map(name => resultDataSet.col(name).contains("~"))

    val condition: Column = allColumns.tail.foldLeft(allColumns.head)((acc: Column, actual: Column) => acc || actual)
    // resultDataSet.col("ZIP").contains("~") || resultDataSet.col("SSN").contains("~")
    val filter = resultDataSet.filter(condition)
    filter.toDF(BlackOakSchema.schema: _*).show(15)


    val outliers: RDD[String] = filter.rdd.flatMap(row => {
      val vals: Map[String, String] = row.getValuesMap[String](BlackOakSchema.schema)
      val filtered = vals.filter(a => !Strings.isNullOrEmpty(a._2) && a._2.contains("~"))
      val idx: List[Int] = BlackOakSchema.getIndexesByAttrNames(filtered.keySet.toList)
      val recID: String = row.getAs[String]("RecID").trim
      val outliersLines: List[String] = idx.map(i => s"$recID,$i")
      outliersLines
    })

    val outliersDS: Dataset[String] = outliers.filter(!_.isEmpty).toDS()
    outliersDS.show(3)

    //todo: write results to folder


    outliersDS.write.text(conf.getString(outputFolder))
    //.coalesce(1)
    //      .write.format("com.databricks.spark.csv")
    //      .save(conf.getString("output.dboost.result.folder"))


    /*
    * // place all data in a single partition
   .coalesce(1)
   .write.format("com.databricks.spark.csv")
   .save("mydata.csv")
    * */


    sparkSession.stop()

  }


  // can be outsourced?
  private def createSession() = {
    SparkSession
      .builder()
      .master("local[4]")
      .appName("F1")
      .config("spark.local.ip",
        conf.getString("spark.config.local.ip.value"))
      .config("spark.driver.memory", "10g")
      .config("spark.executor.memory", "8g")
      .getOrCreate()
  }
}

object DBoostResultWriter {
  def main(args: Array[String]): Unit = {
    new DBoostResultWriter().writeResults()
  }

}
