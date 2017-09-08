package de.evaluation.f1

import java.io.File

import com.typesafe.config.ConfigFactory
import de.evaluation.util.{DataSetCreator, SparkSessionCreator}
import org.apache.spark.sql.DataFrame

object FullResult {
  val recid = "RecID"
  val attrnr = "attrNr"
  val label: String = "label"

  val value: String = "value"

  //todo: extract howManyTools we have
  //val toolsNumber = 5
  val toolsCount = ConfigFactory.load().getInt("tools.available")
  val tools: Seq[String] = (1 to toolsCount).map(i => s"${GoldStandard.exists}-$i")
  val schema = Seq(recid) ++ Seq(attrnr) ++ Seq(label) ++ tools
}

object GoldStandard {
  val recid = "RecID"
  val attrnr = "attrNr"
  val exists = "exists"
  val schema = Seq(recid, attrnr, exists)
}

object Cells {
  val recid = "RecID"
  val attrnr = "attrNr"
  val schema = Seq(recid, attrnr)
}

@Deprecated
class BlackOakF1 {

  def produceEvaluationF1(): Unit = {
    val sparkSession = SparkSessionCreator.createSession("ALL-F1")

    val goldStdFileConfig = "output.blackouak.gold.file"
    val goldStd: DataFrame = DataSetCreator.createDataSetNoHeader(sparkSession, goldStdFileConfig, Cells.schema: _*)

    //goldStd.show(5)

    val dboostResultConf = "output.dboost.result.file"
    val dboostResult: DataFrame = DataSetCreator.createDataSetNoHeader(sparkSession, dboostResultConf, Cells.schema: _*)
    val dboostEval: Eval = F1.evaluateResult(goldStd, dboostResult)

    val dboostGausResultConf = "output.dboost.gaus.result.file"
    val dboostGaus = DataSetCreator.createDataSetNoHeader(sparkSession, dboostGausResultConf, Cells.schema: _*)
    val dboostGausEval = F1.evaluateResult(goldStd, dboostGaus)


    val nadeefResultConf = "output.nadeef.detect.result.file"
    val nadeefResult: DataFrame = DataSetCreator.createDataSetNoHeader(sparkSession, nadeefResultConf, Cells.schema: _*)
    val nadeefEval = F1.evaluateResult(goldStd, nadeefResult)

    dboostEval.printResult("dboost")
    dboostGausEval.printResult("dboost gaus")
    nadeefEval.printResult("nadeef")


    sparkSession.stop()
  }


  def produceEvalForMultipleFiles() = {
    val conf = ConfigFactory.load()
    val outputDir = conf.getString("dboost.small.eval.folder")
    val evalFolder = new File(outputDir)
    val allDirs = evalFolder.listFiles().filter(_.isDirectory).toList

    val sparkSession = SparkSessionCreator.createSession("F1-FOR-DBOOST-SMALL")

    val goldStdFileConfig = "dboost.small.gold.log.file"
    val goldStd: DataFrame = DataSetCreator.createDataSetNoHeader(sparkSession, goldStdFileConfig, Cells.schema: _*)


    allDirs.map(dir => {
      val path: String = dir.getAbsolutePath
      val nameForFile = dir.getName
      val logData = s"$path/$nameForFile.txt"

      val found = DataSetCreator.createFrameNoHeader(sparkSession, logData, Cells.schema: _*)

      val eval = F1.evaluateResult(goldStd, found)
      eval.printResult(nameForFile)

    })
    sparkSession.stop()

  }


}

object BlackOakF1 {
  def main(args: Array[String]): Unit = {
    new BlackOakF1().produceEvaluationF1()
    //new BlackOakF1().produceEvalForMultipleFiles()
  }
}
