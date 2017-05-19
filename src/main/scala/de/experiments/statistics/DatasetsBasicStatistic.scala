package de.experiments.statistics

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.NumbersUtil

/**
  * Created by visenger on 19.05.17.
  */
object DatasetsBasicStatistic {

  def main(args: Array[String]): Unit = {

    val datasets = Seq("output.full.result.file" /*blackoak*/ , "result.hosp.10k.full.result.file", "result.salaries.full.result.file", "result.flights.full.result.file")
    val config = ConfigFactory.load()
    println(s" data set name | errors %")
    SparkLOAN.withSparkSession("STAT") {
      session => {

        datasets.foreach(dsName => {
          val fullDatasetPath = config.getString(dsName)
          val data = DataSetCreator.createFrameNoHeader(session, fullDatasetPath, FullResult.schema: _*)

          val totalCount = data.select(FullResult.label).count()
          val countErrors = data.where(data.col(FullResult.label) === "1").count()

          val errorPercentage = (countErrors.toDouble / totalCount.toDouble) * 100

          println(s" $dsName | ${NumbersUtil.round(errorPercentage)} %")

        })

      }
    }
  }

}

object DataSetSchemaCharacteristic extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    allSchemasByName.foreach(schema => {
      val s = schema._2
      val numOfAttributes = s.getSchema.length
      println(s"${schema._1} | $numOfAttributes")
    })
  }
}
