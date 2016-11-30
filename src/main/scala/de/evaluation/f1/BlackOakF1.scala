package de.evaluation.f1

import de.evaluation.util.{DataSetCreator, SparkSessionCreator}
import org.apache.spark.sql.{DataFrame, Dataset}

object DataF1 {
  val schema = Seq("RecID", "attrNr")
}

class BlackOakF1 {

  def produceEvaluationF1(): Unit = {
    val sparkSession = SparkSessionCreator.createSession("ALL-F1")

    val goldStdFileConfig = "output.blackouak.goldstandard.file"
    val goldStd: DataFrame = DataSetCreator.createDataSetNoHeader(sparkSession, goldStdFileConfig, DataF1.schema: _*)

    //goldStd.show(5)

    val dboostResultConf = "output.dboost.result.file"
    val dboostResult: DataFrame = DataSetCreator.createDataSetNoHeader(sparkSession, dboostResultConf, DataF1.schema: _*)
    val dboostEval: Eval = evaluateResult(goldStd, dboostResult)

    val dboostGausResultConf = "output.dboost.gaus.result.file"
    val dboostGaus = DataSetCreator.createDataSetNoHeader(sparkSession, dboostGausResultConf, DataF1.schema: _*)
    val dboostGausEval = evaluateResult(goldStd, dboostGaus)


    val nadeefResultConf = "output.nadeef.detect.result.file"
    val nadeefResult: DataFrame = DataSetCreator.createDataSetNoHeader(sparkSession, nadeefResultConf, DataF1.schema: _*)
    val nadeefEval = evaluateResult(goldStd, nadeefResult)

    dboostEval.printResult("dboost")
    dboostGausEval.printResult("dboost gaus")
    nadeefEval.printResult("nadeef")


    sparkSession.stop()
  }

  def evaluateResult(goldStandard: DataFrame, selected: DataFrame): Eval = {
    val tpDataset: DataFrame = goldStandard.intersect(selected)
    val tp: Long = tpDataset.count()

    val fnDataset: DataFrame = goldStandard.except(tpDataset)
    val fn = fnDataset.count()

    val fpDataset: DataFrame = selected.except(tpDataset)
    val fp: Long = fpDataset.count()

    //println(s"tp= $tp, fn= $fn, fp=$fp")
    val precision = tp.toDouble / (tp + fp).toDouble
    val recall = tp.toDouble / (tp + fn).toDouble
    val F1 = (2 * precision * recall) / (precision + recall)
    Eval(precision, recall, F1)
  }


}

object BlackOakF1 {
  def main(args: Array[String]): Unit = {
    new BlackOakF1().produceEvaluationF1()
  }
}
