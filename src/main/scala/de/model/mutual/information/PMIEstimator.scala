package de.model.mutual.information

import de.evaluation.f1.{Eval, F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.{Model, NumbersUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.BigDecimal.RoundingMode

/**
  * Created by visenger on 13/01/17.
  */
class PMIEstimator {

  private var path = ""

  def onData(f: String): this.type = {
    path = f
    this
  }

  def runPMI(): Unit = {
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
          //          val pmiWithLog2 = DoubleMath
          //            .log2((cooccuringTools.toDouble * sampleSize.toDouble) / (firstTool.toDouble * secondTool.toDouble))

          ToolPMI(
            tool1,
            tool2,
            NumbersUtil.round(pmi)
          )
        })

        val toolPMIs: List[ToolPMI] = pmis.sortWith((p1, p2) => p1.pmi > p2.pmi).toList

        toolPMIs.foreach(t => println(t.toString))

        val allTops: List[List[String]] = aggregateTools(toolPMIs)
        printEvaluation(model, allTops)

        println(s"inverse PMI:")
        val toolsInversePMIs = toolPMIs.reverse
        val allInvPMIs: List[List[String]] = aggregateTools(toolsInversePMIs)
        printEvaluation(model, allInvPMIs)
      }
    }
  }

  private def getData(session: SparkSession): DataFrame = {
    val data = DataSetCreator.createDataSetNoHeader(session, path, FullResult.schema: _*)
    data
  }

  private def countElementsOfColumn(twoTools: DataFrame, colName: String): Long = {
    twoTools.where(twoTools.col(colName) === "1")
      .count()
  }

  private def printEvaluation(model: DataFrame, allTops: List[List[String]]) = {


    allTops.foreach(topTools => {
      val tools = topTools.mkString(" + ")
//      println(tools)

      val label = FullResult.label
      val labelAndTopTools = model.select(label, topTools: _*)

      val eval: Eval = F1.evaluate(labelAndTopTools)
//      eval.printResult("union all")

      val k = topTools.length
      val minK: Eval = F1.evaluate(labelAndTopTools, k)
//      minK.printResult(s"min-$k")

      val latexTableRow =
        s"""
                  \\multirow{2}{*}{Top-1} & \\multirow{2}{*}{$tools} & union all  & ${eval.precision}        & ${eval.recall}     & ${eval.f1}  \\\\
                             &                              & min-$k      & ${minK.precision}        & ${minK.recall}     & ${minK.f1}   \\\\
          """.stripMargin
            println(latexTableRow)
    })
  }

  private def aggregateTools(toolPMIs: List[ToolPMI]): List[List[String]] = {
    val allTops = (1 to 5).map(i => {
      val top1 = toolPMIs.take(i)
      val listOfTools = getListOfTopPMIs(top1)
      listOfTools
    }).toSet.toList
    allTops
  }

  private def getListOfTopPMIs(top: List[ToolPMI]): List[String] = {
    val tools: Set[String] = top.flatMap(t => {
      Seq(t.tool1, t.tool2)
    }).toSet
    tools.toList
  }
}

case class ToolPMI(tool1: String, tool2: String, pmi: Double) {
  override def toString: String = {
    s"$tool1, $tool2, PMI: $pmi"
  }
}

object HospPMIEstimatorRunner {
  val resultPath = "result.hosp.10k.full.result.file"

  def main(args: Array[String]): Unit = {
    val pmi = new PMIEstimator()
    pmi.onData(resultPath)
    pmi.runPMI()
  }
}


object PMIEstimatorRunner {
  val resultPath = "output.full.result.file"

  def main(args: Array[String]): Unit = {
    val pmi = new PMIEstimator()
    pmi.onData(resultPath)
    pmi.runPMI()
  }

  def old_main(args: Array[String]): Unit = {
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
          //          val pmiWithLog2 = DoubleMath
          //            .log2((cooccuringTools.toDouble * sampleSize.toDouble) / (firstTool.toDouble * secondTool.toDouble))

          ToolPMI(
            tool1,
            tool2,
            round(pmi)
          )
        })

        val toolPMIs: List[ToolPMI] = pmis.sortWith((p1, p2) => p1.pmi > p2.pmi).toList

        toolPMIs.foreach(t => println(t.toString))

        val allTops: List[List[String]] = aggregateTools(toolPMIs)
        printEvaluation(model, allTops)

        println(s"inverse PMI:")
        val toolsInversePMIs = toolPMIs.reverse
        val allInvPMIs: List[List[String]] = aggregateTools(toolsInversePMIs)
        printEvaluation(model, allInvPMIs)
      }
    }


  }


  def getData(session: SparkSession): DataFrame = {
    val path = "output.full.result.file"
    val blackOakFullResult = DataSetCreator.createDataSetNoHeader(session, path, FullResult.schema: _*)
    blackOakFullResult
  }

  private def countElementsOfColumn(twoTools: DataFrame, colName: String): Long = {
    twoTools.where(twoTools.col(colName) === "1")
      .count()
  }

  private def printEvaluation(model: DataFrame, allTops: List[List[String]]) = {


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

      //      val latexTableRow =
      //        s"""
      //            \\multirow{2}{*}{Top-1} & \\multirow{2}{*}{$tools} & union all  & ${eval.precision}        & ${eval.recall}     & ${eval.f1}  \\\\
      //                       &                              & min-$k      & ${minK.precision}        & ${minK.recall}     & ${minK.f1}   \\\\
      //    """.stripMargin
      //      println(latexTableRow)
    })
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


  private def sortBy(pmis: List[ToolPMI])(predicate: Tuple2[ToolPMI, ToolPMI] => Boolean) = {
    pmis.sortWith((t1, t2) => {
      predicate.apply(t1, t2)
      // t1.pmi < t2.pmi
    })
  }


  def round(percentageFound: Double, scale: Int = 2) = {
    BigDecimal(percentageFound).setScale(scale, RoundingMode.HALF_UP).toDouble
  }


}

@Deprecated
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
