package de.experiments.statistics

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.NumbersUtil
import org.apache.spark.rdd.RDD

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

object SystemsPerformanceStatistic {
  def main(args: Array[String]): Unit = {

    val datasets = Seq(/*"output.full.result.file" /*blackoak*/ , "result.hosp.10k.full.result.file",*/ "result.salaries.full.result.file" /*, "result.flights.full.result.file"*/)
    val config = ConfigFactory.load()

    SparkLOAN.withSparkSession("TOOLS-STAT") {
      session => {
        datasets.foreach(dataset => {
          val datasetPath = config.getString(dataset)
          val dataFrame = DataSetCreator.createFrame(session, datasetPath, FullResult.schema: _*)

          val toolsPerformance = dataFrame
            .select(FullResult.label, FullResult.attrnr)
            .where(dataFrame.col(FullResult.label) === "1")

          val totalErrors = toolsPerformance.count()

          val tools: RDD[(Int, Int)] = toolsPerformance.rdd.map(row => {
            val label: Int = row.getString(0).toInt
            val attrnr: Int = row.getString(1).toInt

            (label, attrnr)
          })
          val systemsCombiCount = tools.countByValue()

          println(s"DATASET: $dataset")
          println(s"label, attr-nr: count")

          systemsCombiCount.foreach(e => {
            val errorPercentage = (e._2.toDouble / totalErrors.toDouble) * 100
            val attrNr = e._1._2
            println(s"${e._1._1}, ${attrNr}: ${e._2} / ${NumbersUtil.round(errorPercentage)}%")
          })


        })
      }
    }

  }
}

object SystemsPerformanceList extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {

    val mapDatasets = Map(
      "blackoak"->"output.full.result.file",
      "hosp" -> "result.hosp.10k.full.result.file",
      "salaries" -> "result.salaries.full.result.file",
      "flights" -> "result.flights.full.result.file")

    val config = ConfigFactory.load()

    SparkLOAN.withSparkSession("TOOLS-STAT") {
      session => {
        mapDatasets.foreach(dataset => {
          val path = dataset._2
          val datasetPath = config.getString(path)
          val dataFrame = DataSetCreator.createFrame(session, datasetPath, FullResult.schema: _*)
          import org.apache.spark.sql.functions._

          FullResult.tools.foreach(t => {
            val systemX = dataFrame
              .where(dataFrame.col(FullResult.label) === dataFrame.col(t))
              .withColumn(s"list-$t", concat(dataFrame.col(FullResult.recid), lit("-"), dataFrame.col(FullResult.attrnr)))
              .select(s"list-$t")

            systemX.show(false)

            val homeDir = config.getString(s"home.dir.${dataset._1}")
            val outputPath = s"$homeDir/system-total-performance-lists/list-${getName(t)}"

            systemX
              .coalesce(1)
              .write
              .csv(outputPath)


          })
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
