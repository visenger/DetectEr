package de.model.util

import com.typesafe.config.ConfigFactory
import de.evaluation.f1.{FullResult, GoldStandard}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 28/12/16.
  */
class LibsvmConverter {

  val fullResultPath = "result.salaries.full.result.file"
  //"output.full.result.file"
  val libsvmFolder = "model.salaries.libsvm.folder" //"model.full.result.folder"

  def toLogisticRegrLibsvm(): Unit = {
    SparkLOAN.withSparkSession("LRLIBSVM") {
      session => {
        val matrixDF = DataSetCreator.createDataSet(session, fullResultPath, FullResult.schema: _*)
        val libsvm: DataFrame = toLibsvmFormat(matrixDF)
        libsvm.show(78)

        val path = ConfigFactory.load().getString(libsvmFolder)
        libsvm
          .coalesce(1)
          .write
          .text(path)

      }
    }

  }

  @Deprecated
  def convertCsvToLibsvm(): Unit = {
    SparkLOAN.withSparkSession("LIBSVM") {
      session => {
        val matrixDF = DataSetCreator.createDataSet(session, "model.matrix.file", Model.schema: _*)

        //todo: convert categorical features to doubles

        val indexerModel = new StringIndexer()
          .setInputCol(Model.recId)
          .setOutputCol(Model.indexedcol)
          .fit(matrixDF)
        val categoricalToIdx = indexerModel.transform(matrixDF)

        val libsvm: DataFrame = createLibsvmFormat(categoricalToIdx)

        val path = ConfigFactory.load().getString("model.libsvm.folder")
        libsvm
          .coalesce(1)
          .write
          .text(path)
      }
    }
  }

  private def toLibsvmFormat(matrixWithLabel: DataFrame): DataFrame = {

    val libsvm: RDD[String] = matrixWithLabel.rdd.map(row => {
      val values: Map[String, String] = row.getValuesMap[String](FullResult.schema)
      val partition: (Map[String, String], Map[String, String]) = values.partition(_._1.startsWith(GoldStandard.exists))
      val tools: Map[String, String] = partition._1

      val setValues: Map[String, String] = tools.partition(_._2 != "0")._1

      val emptyRow = ""
      setValues.isEmpty match {
        case true => emptyRow
        case false => {

          val label = values.getOrElse(FullResult.label, "0")
          val featuresNr = setValues.keySet.map(k => k.split("-")(1)).toSeq.sorted
          val features = featuresNr.mkString("", ":1 ", ":1")
          val libsvmRow = s"$label $features"
          // s"$label tool:1...."
          libsvmRow
        }
      }

    })
    import matrixWithLabel.sparkSession.implicits._

    val nonEmptyRows = libsvm.filter(_ != "")
    nonEmptyRows.toDF("row")
  }

  @Deprecated
  private def createLibsvmFormat(matrixDF: DataFrame): DataFrame = {

    val libsvm: RDD[String] = matrixDF.rdd.map(row => {
      val values: Map[String, String] = row.getValuesMap[String](Model.extendedSchema)

      val partition: (Map[String, String], Map[String, String]) = values.partition(_._1.startsWith("exists"))

      val tools: Map[String, String] = partition._1

      /*we take only these where values are set*/
      val setValues: Map[String, String] = tools.partition(_._2 != "0")._1

      val emptyRow = ""
      setValues.isEmpty match {
        case true => emptyRow
        case false => {
          val ids: Map[String, _] = partition._2

          //value is double because we converted categorical features into doubles
          val valueIdx = ids.getOrElse(Model.indexedcol, 0.0)
          val recidIdx = String.valueOf(valueIdx)

          val attr = ids.getOrElse(Model.attrNr, "0")

          val toolNrs: Seq[String] = setValues.keySet.map(k => k.split("-")(1)).toSeq.sorted
          val colsAndVals: String = toolNrs.mkString("", s":$recidIdx ", s":$recidIdx")
          val labeledRow = s"$attr $colsAndVals"

          // s"$attr tool:recId...."
          labeledRow
        }
      }


    })

    import matrixDF.sparkSession.implicits._

    val nonEmptyRows = libsvm.filter(_ != "")
    nonEmptyRows.toDF("row")
  }
}

object LibsvmConverterRunner {
  def main(args: Array[String]): Unit = {
    new LibsvmConverter().toLogisticRegrLibsvm()
  }

}
