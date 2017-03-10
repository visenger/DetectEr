package de.experiments.statistics

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.cosine.similarity.{AllToolsSimilarity, Cosine}
import de.model.kappa.{Kappa, KappaEstimator}
import de.model.mutual.information.{PMIEstimator, ToolPMI}
import org.apache.spark.sql.DataFrame

/**
  * Created by visenger on 08/03/17.
  */
class AllSimilarities {

}

object Similarities extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    val pmiEstimator = new PMIEstimator()
    val kappaEstimator = new KappaEstimator()
    val toolsSimilarity = new AllToolsSimilarity()
    val toolsPairs: List[(String, String)] = FullResult.tools.combinations(2).map(c => (c(0), c(1))).toList

    SparkLOAN.withSparkSession("ALL-TOOLS-SIMIS") {
      session => {
        process_train_data {
          data => {
            val datasetName = data._1
            val filePath = data._2

            val fullResult: DataFrame = DataSetCreator.createFrame(session, filePath, FullResult.schema: _*)
            val allMeasures: List[(ToolPMI, Kappa, Cosine)] = toolsPairs.map(pair => {
              val tools = Seq(pair._1, pair._2)
              val pmi: ToolPMI = pmiEstimator.computePMI(fullResult, tools)
              val kappa = kappaEstimator.computeKappa(fullResult, tools)
              val cosine = toolsSimilarity.computeCosine(session, fullResult, pair)
              (pmi, kappa, cosine)
            })
            val sortedByCousine = allMeasures.sortWith((t1, t2) => t1._3.similarity > t2._3.similarity)

            println(datasetName)

            sortedByCousine.foreach(simis => {
              val pmi = simis._1
              val kappa = simis._2
              val cosine = simis._3
              val latexString = s"""${getName(pmi.tool1)} & ${getName(pmi.tool2)} & ${pmi.pmi} & ${kappa.kappa} & ${cosine.similarity} \\\\"""

              println(latexString)
            })


          }
        }
      }
    }
  }
}
