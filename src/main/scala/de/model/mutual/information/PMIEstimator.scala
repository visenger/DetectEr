package de.model.mutual.information

import com.google.common.math.DoubleMath
import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.Model
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.immutable.IndexedSeq
import scala.math.BigDecimal.RoundingMode

/**
  * Created by visenger on 13/01/17.
  */
class PMIEstimator {

}

case class ToolPMI(tool1: String, tool2: String, pmi: Double, pmiLog2: Double) {
  override def toString: String = {
    s"$tool1, $tool2, PMI: $pmi, PMI-log2: $pmiLog2"
  }
}

object PMIEstimatorRunner {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("PMI") {
      session => {
        val model: DataFrame = getData(session)

        //        val tool1 = "exists-1"
        //        val tool2 = "exists-2"

        val pairs: List[Seq[String]] = FullResult.tools.combinations(2).toList

        val pmis: List[ToolPMI] = pairs.map(t => {
          val tool1 = t(0)
          val tool2 = t(1)

          val twoTools = model.select(model.col(FullResult.label), model.col(tool1), model.col(tool2))

          val firstTool = countElementsOfColumn(twoTools, tool1)
          val secondTool = countElementsOfColumn(twoTools, tool2)

          val cooccuringTools = twoTools
            .where(twoTools.col(tool1) === "1" && twoTools.col(tool2) === "1")
            .count()

          val sampleSize = model.count()
          val pmi: Double = Math
            .log((cooccuringTools.toDouble * sampleSize.toDouble) / (firstTool.toDouble * secondTool.toDouble))
          val pmiWithLog2 = DoubleMath
            .log2((cooccuringTools.toDouble * sampleSize.toDouble) / (firstTool.toDouble * secondTool.toDouble))

          ToolPMI(
            tool1,
            tool2,
            round(pmi),
            round(pmiWithLog2))
        })

        val toolPMIs: List[ToolPMI] = pmis.sortWith((p1, p2) => p1.pmi > p2.pmi).toList

        toolPMIs.foreach(t => println(t.toString))

        val allTops: List[List[String]] = aggregateTools(toolPMIs)

        allTops.foreach(topTools => {
          val tools = topTools.mkString(" + ")
          println(tools)

          val label = FullResult.label
          val labelAndTopTools = model.select(label, topTools: _*)

          val eval: Eval = F1.evaluate(labelAndTopTools)
          eval.printResult("union all")

          val k = topTools.length
          val minK: Eval = F1.evaluate(labelAndTopTools, k)
          minK.printResult(s"min-$k")
        })


      }
    }


  }


  def aggregateTools(toolPMIs: List[ToolPMI]): List[List[String]] = {
    val allTops = (1 to 5).map(i => {
      val top1 = toolPMIs.take(i)
      val listOfTools = getListOfTopPMIs(top1)
      listOfTools
    }).toSet.toList
    allTops
  }

  def getListOfTopPMIs(top: List[ToolPMI]): List[String] = {
    val tools: Set[String] = top.flatMap(t => {
      Seq(t.tool1, t.tool2)
    }).toSet
    tools.toList
  }

  def getData(session: SparkSession): DataFrame = {
    val blackOakFullResult = DataSetCreator.createDataSetNoHeader(session, "output.full.result.file", FullResult.schema: _*)
    blackOakFullResult
  }

  private def sortBy(pmis: List[ToolPMI])(predicate: Tuple2[ToolPMI, ToolPMI] => Boolean) = {
    pmis.sortWith((t1, t2) => {
      predicate.apply(t1, t2)
      // t1.pmi < t2.pmi
    })
  }

  @Deprecated
  private def computePercentageFoundErrors(model: DataFrame, pmis: List[ToolPMI]): String = {

    val firstWithLowPMI: Set[String] = pmis.flatMap(t => {
      Seq(t.tool1, t.tool2)
    }).toSet

    firstWithLowPMI.foreach(println)


    val cols: Seq[Column] = firstWithLowPMI.map(t => model.col(t)).toSeq
    val toolsWithLowPMI = model.select(cols: _*)

    val countTools = firstWithLowPMI.size

    val nothingFound = toolsWithLowPMI.filter(row => {
      val values = (0 to countTools - 1).map(i => row.getString(i).toInt)
      values.foldLeft(true)((acc, v) => acc && v == 0)
    }).count()

    val sampleSize: Long = model.count()
    val somethingFound = sampleSize - nothingFound

    val percentageFoundByTools = somethingFound.toDouble * 100 / sampleSize.toDouble

    val roundPercentFoungByTools = round(percentageFoundByTools)

    s"""  Selected Tools : ${firstWithLowPMI.mkString(",")},
       | the total percentage of errors found is: $roundPercentFoungByTools
           """.stripMargin

  }

  def round(percentageFound: Double, scale: Int = 2) = {
    BigDecimal(percentageFound).setScale(scale, RoundingMode.HALF_UP).toDouble
  }

  private def countElementsOfColumn(twoTools: DataFrame, colName: String): Long = {
    twoTools.where(twoTools.col(colName) === "1")
      .count()
  }
}

object RandomInformation {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("RNDINF") {
      session => {
        val model: DataFrame = DataSetCreator.createDataSet(session, "model.matrix.file", Model.schema: _*)

        val sampleSize = model.count()

        val tools = Model.tools.map(t => model.col(t))

        val toolsSelect = model.select(tools: _*)

        val notFoundErrorsCount = toolsSelect.filter(row => {
          val notFound1 = row.getString(0).toInt == 0
          val notFound2 = row.getString(1).toInt == 0
          val notFound3 = row.getString(2).toInt == 0
          val notFound4 = row.getString(3).toInt == 0
          val notFound5 = row.getString(4).toInt == 0
          notFound1 && notFound2 && notFound3 && notFound4 && notFound5
        }).count()
        println(s" errors not found: $notFoundErrorsCount from $sampleSize")

        val found: Long = sampleSize - notFoundErrorsCount

        val foundPercent: Double = BigDecimal(found.toDouble * 100 / sampleSize.toDouble).setScale(2, RoundingMode.HALF_UP).toDouble
        println(s" found percent: $foundPercent %")


      }
    }
  }
}
