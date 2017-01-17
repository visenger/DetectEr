package de.model.mutual.information

import com.google.common.math.DoubleMath
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.model.util.Model
import org.apache.spark.sql.{Column, DataFrame}

import scala.math.BigDecimal.RoundingMode

/**
  * Created by visenger on 13/01/17.
  */
class PMIEstimator {

}

case class ToolPMI(tool1: String, tool2: String, pmi: Double, pmiLog2: Double, percentFound: Double)

object PMIEstimatorRunner {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("PMI") {
      session => {
        val model: DataFrame = DataSetCreator.createDataSet(session, "model.matrix.file", Model.schema: _*)

        val sampleSize = model.count()


        //        val tool1 = "exists-1"
        //        val tool2 = "exists-2"

        val pairs: List[Seq[String]] = Model.tools.combinations(2).toList

        val pmis: List[ToolPMI] = pairs.map(t => {
          val tool1 = t(0)
          val tool2 = t(1)

          val twoTools = model.select(model.col(tool1), model.col(tool2))

          val firstTool = countElementsOfColumn(twoTools, 0)

          val secondTool = countElementsOfColumn(twoTools, 1)

          val cooccuringTools = twoTools
            .filter(row => {
              val firstElement = row.getString(0).toInt == 1
              val secondElement = row.getString(1).toInt == 1
              firstElement && secondElement

            }).count()


          val bothToolsNothing = twoTools
            .filter(row => {
              val firstElement = row.getString(0).toInt == 0
              val secondElement = row.getString(1).toInt == 0
              firstElement && secondElement

            }).count()

          val found = sampleSize - bothToolsNothing

          val percentageFound: Double = found.toDouble * 100 / sampleSize.toDouble

          // println(s" First Tool [$tool1] found: $firstTool")
          // println(s" Second Tool [$tool2] found: $secondTool")
          // println(s" Coocurence of [$tool1] and [$tool2]: $cooccuringTools")
          // println(s" Found total by [$tool1] and [$tool2]: $percentageFound%")


          val pmi: Double = Math.log((cooccuringTools.toDouble * sampleSize.toDouble) / (firstTool.toDouble * secondTool.toDouble))
          val pmiWithLog2 = DoubleMath.log2((cooccuringTools.toDouble * sampleSize.toDouble) / (firstTool.toDouble * secondTool.toDouble))

          // println(s" PMI of [$tool1] and [$tool2]  tools: $pmi")
          // println(s" LOG2 PMI of [$tool1] and [$tool2]  tools: $pmiWithLog2")

          // println(s" ------******------ ")

          ToolPMI(
            tool1,
            tool2,
            round(pmi),
            round(pmiWithLog2),
            round(percentageFound))

        })

        println(s" sorting by pmi")
        val sortedByPMI = sortBy(pmis) {
          t => t._1.pmi < t._2.pmi
        }
        sortedByPMI.foreach(println)

        val take = 3
        val firstPMIs = sortedByPMI.take(take)
        println(computePercentageFoundErrors(model, firstPMIs))


        println(s" ---- apply another sorting: by percentage ")
        val sortedByPercentage = sortBy(pmis) {
          t => (t._1.percentFound > t._2.percentFound)
        }
        sortedByPercentage.foreach(println)

        val percentage = sortedByPercentage.take(take)
        println(computePercentageFoundErrors(model, percentage))

        println(s" -- intersect of pmi and percentage ---")
        val intersect = firstPMIs.intersect(percentage)
        println(computePercentageFoundErrors(model, intersect))

        println(s" -- union of pmi and percentage ---")
        val unifiedPMIAndPercent = firstPMIs.union(percentage).toSet.toList
        println(computePercentageFoundErrors(model, unifiedPMIAndPercent))


        println(s" two fold sorting: ")
        val twoCriteriasSorting = sortBy(pmis) {
          t => (t._1.pmi < t._2.pmi) && (t._1.percentFound > t._2.percentFound)
        }
        val pmisAndPercentage = twoCriteriasSorting.take(take)
        println(computePercentageFoundErrors(model, pmisAndPercentage))


      }
    }


  }

  private def sortBy(pmis: List[ToolPMI])(predicate: Tuple2[ToolPMI, ToolPMI] => Boolean) = {
    pmis.sortWith((t1, t2) => {
      predicate.apply(t1, t2)
      // t1.pmi < t2.pmi
    })
  }

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

  private def round(percentageFound: Double) = {
    BigDecimal(percentageFound).setScale(2, RoundingMode.HALF_UP).toDouble
  }

  private def countElementsOfColumn(twoTools: DataFrame, colNum: Int): Long = {
    twoTools
      .filter(row => {
        row.getString(colNum).toInt == 1
      }).count()
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
