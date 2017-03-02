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

  private var train: String = ""
  private var test: String = ""

  private var path = ""

  def addTrainData(trainPath: String): this.type = {
    train = trainPath
    this
  }

  def addTestData(testPath: String): this.type = {
    test = testPath
    this
  }

  def onFullData(f: String): this.type = {
    path = f
    this
  }


  def runPMIOnTrainAndTest(): Unit = {
    SparkLOAN.withSparkSession("TRAIN-PMI-TEST-PMI") {
      session => {
        val trainData: DataFrame = DataSetCreator.createFrame(session, train, FullResult.schema: _*)
        val testData: DataFrame = DataSetCreator.createFrame(session, test, FullResult.schema: _*)
        runPMIOnData(session, trainData, testData)
      }
    }
  }

  def runPMI(): Unit = {
    SparkLOAN.withSparkSession("PMI") {
      session => {
        val model: DataFrame = getData(session)
        runPMIOnData(session, model)
      }
    }
  }

  private def runPMIOnData(session: SparkSession, train: DataFrame, test: DataFrame = null) = {

    val testData: DataFrame = if (test == null) train else test


    //        val tool1 = "exists-1"
    //        val tool2 = "exists-2"

    val pairs: List[Seq[String]] = FullResult.tools.combinations(2).toList
    //todo: model is the train data
    val pmis: List[ToolPMI] = pairs.map(t => {
      val pmi = computePMI(train, t)
      pmi
    })

    val toolPMIs: List[ToolPMI] = pmis.sortWith((p1, p2) => p1.pmi > p2.pmi).toList

    toolPMIs.foreach(t => println(t.toString))

    val allTops: List[List[String]] = aggregateTools(toolPMIs)
    //    todo: the following dataframe should be TEST data (from separate file)
    //printEvaluation(train, allTops)
    printEvaluation(testData, allTops)

    println(s"inverse PMI:")
    val toolsInversePMIs = toolPMIs.reverse
    val allInvPMIs: List[List[String]] = aggregateTools(toolsInversePMIs)
    // printEvaluation(train, allInvPMIs)
    printEvaluation(testData, allInvPMIs)
  }

  def computePMI(model: DataFrame, pairOfTools: Seq[String]): ToolPMI = {
    val tool1 = pairOfTools(0)
    val tool2 = pairOfTools(1)

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
  }

  private def getData(session: SparkSession): DataFrame = {
    val data = DataSetCreator.createDataSetFromCSV(session, path, FullResult.schema: _*)
    data
  }

  private def countElementsOfColumn(twoTools: DataFrame, colName: String): Long = {
    twoTools.where(twoTools.col(colName) === "1")
      .count()
  }

  private def printEvaluation(model: DataFrame, allTops: List[List[String]]) = {


    allTops.foreach(topTools => {
      val tools = topTools.mkString(" + ")
      // println(tools)

      val label = FullResult.label
      val labelAndTopTools = model.select(label, topTools: _*)

      val unionAll: Eval = F1.evaluate(labelAndTopTools)
      // eval.printResult("union all")

      val k = topTools.length
      val minK: Eval = F1.evaluate(labelAndTopTools, k)
      // minK.printResult(s"min-$k")

      val latexTableRow =
        s"""
          \\multirow{2}{*}{} & \\multirow{2}{*}{$tools} & union all  & ${unionAll.precision}        & ${unionAll.recall}     & ${unionAll.f1}  \\\\
                                   &                              & min-$k      & ${minK.precision}        & ${minK.recall}     & ${minK.f1}   \\\\
          """.stripMargin
      println(latexTableRow)
    })
  }

  private def aggregateTools(toolPMIs: List[ToolPMI]): List[List[String]] = {
    val numOfTools = 5
    //todo: check toolPMIs size <= numOfTools
    val allTops = (1 to numOfTools).map(i => {
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

  def run() = {
    println(s"DATASET: HOSP")
    val pmi = new PMIEstimator()
    pmi.onFullData(resultPath)
    pmi.runPMI()
  }

  def main(args: Array[String]): Unit = {
    run()
  }
}

object SalariesPMIEstimatorRunner {
  val resultPath = "result.salaries.full.result.file"

  def run() = {
    println(s"DATASET: SALARIES")
    val pmi = new PMIEstimator()
    pmi.onFullData(resultPath)
    pmi.runPMI()
  }

  def main(args: Array[String]): Unit = {
    run()
  }
}

object PMIEstimatorRunner {
  val resultPath = "output.full.result.file"

  def run() = {
    println(s"DATASET: BLACKOAK")
    val pmi = new PMIEstimator()
    pmi.onFullData(resultPath)
    pmi.runPMI()
  }

  def main(args: Array[String]): Unit = {
    run()
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
    val blackOakFullResult = DataSetCreator.createDataSetFromCSV(session, path, FullResult.schema: _*)
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
