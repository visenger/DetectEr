package de.experiments.baseline

import de.evaluation.f1.{F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.FormatUtil
import org.apache.spark.sql.functions._

/**
  * Created by visenger on 29/03/17.
  */
class MaxKStrategy {

}

object MaxKStrategyRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("MAX-K") {
      session => {
        import session.implicits._

        val dataSetName = "ext.blackoak"
        val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
        val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

        val maximumF1 = experimentsConf.getDouble(s"${dataSetName}.max.F1")
        val minimumF1 = experimentsConf.getDouble(s"${dataSetName}.min.F1")

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        //val testDF = DataSetCreator.createFrame(session, extBlackoakTestFile, FullResult.schema: _*)

        val allTools = FullResult.tools
        val featuresCol = "features"
        val trainLabeledPointsTools = FormatUtil
          .prepareDataToLabeledPoints(session, trainDF, allTools)
          .toDF(FullResult.label, featuresCol)

        val majorityVoter = udf { (tools: org.apache.spark.mllib.linalg.Vector) => {
          val total = tools.size
          val sum1 = tools.numNonzeros
          var sum0 = total - sum1
          val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
          errorDecision
        }
        }
        val majorityVoteCol = "majority-vote"
        var maxKDF = trainLabeledPointsTools
          .withColumn(majorityVoteCol, majorityVoter(trainLabeledPointsTools(featuresCol)))

        val maxKRdd = FormatUtil.getPredictionAndLabel(maxKDF, majorityVoteCol)
        val eval = F1.evalPredictionAndLabels(maxKRdd)
        eval.printResult(s"majority vote eval")


      }
    }
  }
}
