package de.experiments.similarities

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.cosine.similarity.{AllToolsSimilarity, Cosine}
import de.model.kappa.{Kappa, KappaEstimator}
import de.model.mutual.information.{PMIEstimator, ToolPMI}

/**
  * Created by visenger on 24/03/17.
  */
class ExperimentsWithSimis {

}


object SimilaritiesOnTruthMatrixRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("WAHRHEITSMATRIX2") {
      session => {

        import org.apache.spark.sql.functions._

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)

        val nxor = udf { (label: String, tool: String) => if (label.equals(tool)) "1" else "0" }

        var truthDF = trainDF

        val tools = FullResult.tools
        tools.foreach(tool => {
          truthDF = truthDF
            .withColumn(s"truth-$tool", nxor(trainDF("label"), trainDF(tool)))
        })

        val truthTools: Seq[String] = tools.map(t => s"truth-$t")


        var labelAndTruth = truthDF.select(FullResult.label, truthTools: _*)
        tools.foreach(tool => {
          labelAndTruth = labelAndTruth.withColumnRenamed(s"truth-$tool", tool)
        })

        val labelAndTools = Seq(FullResult.label) ++ tools

        val allMetrics: List[(ToolPMI, Kappa, Cosine)] = labelAndTools
          .combinations(2)
          .map(pair => {

            val tool1 = pair(0)
            val tool2 = pair(1)

            val pmi: ToolPMI = new PMIEstimator().computePMI(labelAndTruth, Seq(tool1, tool2))
            val kappa: Kappa = new KappaEstimator().computeKappa(labelAndTruth, Seq(tool1, tool2))
            val cosine: Cosine = new AllToolsSimilarity().computeCosine(session, labelAndTruth, (tool1, tool2))

            (pmi, kappa, cosine)
          }).toList

        println(s"ALL METRICS PERFORMED ON TRUTH MATRIX")
        allMetrics
          .sortBy(trio => trio._1.pmi)
          //.sortBy(trio => trio._3.similarity)
          .reverse
          .foreach(trio => {

            val pmi = trio._1
            val kappa = trio._2
            val cosine = trio._3

            val tool1 = pmi.tool1
            val tool2 = pmi.tool2

            println(s"${getExtName(tool1)} _ ${getExtName(tool2)} PMI: ${pmi.pmi}, KAPPA: ${kappa.kappa}, COS: ${cosine.similarity}")
          })

      }
    }
  }
}

object SimilaritiesOnErrorMatrixRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("ERRORMATRIX") {
      session => {

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        val tools = Seq(FullResult.label) ++ FullResult.tools

        val allMetrics: List[(ToolPMI, Kappa, Cosine)] = tools.combinations(2).map(pair => {

          val tool1 = pair(0)
          val tool2 = pair(1)

          val pmi: ToolPMI = new PMIEstimator().computePMI(trainDF, Seq(tool1, tool2))
          val kappa: Kappa = new KappaEstimator().computeKappa(trainDF, Seq(tool1, tool2))
          val cosine: Cosine = new AllToolsSimilarity().computeCosine(session, trainDF, (tool1, tool2))

          (pmi, kappa, cosine)
        }).toList

        println(s"ALL METRICS PERFORMED ON ERROR MATRIX")
        allMetrics
          //          .sortBy(trio => trio._1.pmi)
          .sortBy(trio => trio._3.similarity)
          .reverse
          .foreach(trio => {

            val pmi = trio._1
            val kappa = trio._2
            val cosine = trio._3

            val tool1 = pmi.tool1
            val tool2 = pmi.tool2

            println(s"${getExtName(tool1)} _ ${getExtName(tool2)} PMI: ${pmi.pmi}, KAPPA: ${kappa.kappa}, COS: ${cosine.similarity}")
          })

      }
    }
  }
}

