package de.model.util

import com.typesafe.config.ConfigFactory
import de.evaluation.data.blackoak.{BlackOakGoldStandard, BlackOakSchema}
import de.evaluation.f1.DataF1
import de.evaluation.tools.deduplication.nadeef.NadeefDeduplicationResults
import de.evaluation.tools.outliers.dboost.DBoostResults
import de.evaluation.tools.pattern.violation.TrifactaResults
import de.evaluation.tools.ruleviolations.nadeef.NadeefRulesVioResults
import de.evaluation.util.{DataSetCreator, DatabaseProps, SparkLOAN, SparkSessionCreator}
import org.apache.spark.sql._


/**
  * Created by visenger on 22/12/16.
  */
class ResultCruncher {


  def proofLoan() = {
    import SparkLOAN._
    // disadvanatage: with loan pattern we are not able to use the data frame objects outside the scope
    // of the method
    // loan pattern ist suitable for performing something without returning result. -> void methods.
    // e.g side-effect methods, which perform I/O;
    withSparkSession("cruncher") {
      sparkSession => {
        val goldStandard: DataFrame= BlackOakGoldStandard
          .getGoldStandard(sparkSession)
        goldStandard.show(1)

        val patternViolationResult: DataFrame = TrifactaResults
          .getPatternViolationResult(sparkSession)
        patternViolationResult.show(2)


        val dedupResult: DataFrame = NadeefDeduplicationResults
          .getDedupResults(sparkSession)
        dedupResult.show(3)

        val histOutliers = DBoostResults
          .getHistogramResultForOutlierDetection(sparkSession)
        histOutliers.show(4)

        val gaussOutliers = DBoostResults
          .getGaussResultForOutlierDetection(sparkSession)
        gaussOutliers.show(5)

        val rulesVioResults = NadeefRulesVioResults
          .getRulesVioResults(sparkSession)
        rulesVioResults.show(6)

      }

    }


    /**
      *
      * def withSparkSession(name: String)(f: SparkSession => Unit) = {
      * val r = SparkSessionCreator.createSession(name)
      * try {
      * f(r)
      * } finally {
      *r.stop()
      * }
      * }
      *
      **/


  }


}

object ResultCruncher {
  def main(args: Array[String]): Unit = {

    new ResultCruncher().proofLoan()
  }

}
