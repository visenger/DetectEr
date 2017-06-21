package de.experiments.baseline

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.models.combinator.{Bagging, Stacking}

/**
  * Created by visenger on 20.06.17.
  */
object NaiveBestKSystems extends ExperimentsCommonConfig {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("NAIVE-BEST-K") {
      session => {

        process_data {
          data => {
            val dataSetName = data._1
            val trainFile = data._2._1
            val testFile = data._2._2

            val trainDF = DataSetCreator.createFrame(session, trainFile, FullResult.schema: _*)
            //val Array(trainDF, _) = trainDFFull.randomSplit(Array(0.1, 0.9))

            val testDF = DataSetCreator.createFrame(session, testFile, FullResult.schema: _*)

            val systemsToPrecision: Seq[(String, Double)] = FullResult.tools.map(system => {
              val p = experimentsConf.getDouble(s"$dataSetName.$system.precision")
              system -> p
            })
            val allSystemsSorted = systemsToPrecision.sortWith((a, b) => a._2 > b._2)

            var bestTools: Seq[String] = Seq()

            (2 to (FullResult.tools.size - 1)).foreach(k => {

              bestTools = allSystemsSorted.take(k).map(_._1)

              println(s" $dataSetName $k-best tools: ${bestTools.map(getName(_)).mkString(splitter)}")

              val clustersBagging = new Bagging()
              val evalNaiveBagging = clustersBagging
                .onDataSetName(dataSetName)
                .useTools(bestTools)
                .onTrainDataFrame(trainDF)
                .onTestDataFrame(testDF)
                .performEnsambleLearningOnTools(session)

              evalNaiveBagging.printResult(s"BAGGING: NAIVE BEST TOOLS ON $dataSetName")

              val baggingWithMeta = new Bagging()
              val evalMetaBagging = baggingWithMeta
                .onDataSetName(dataSetName)
                .useTools(bestTools)
                .onTrainDataFrame(trainDF)
                .onTestDataFrame(testDF)
                .performEnsambleLearningOnToolsAndMetadata(session)
              evalMetaBagging.printResult(s"BAGGING & METADATA: NAIVE BEST TOOLS ON $dataSetName")
              // println(s"bagging: ${k}\t${evalNaiveBagging.precision}\t${evalNaiveBagging.recall}\t${evalNaiveBagging.f1}\t${evalMetaBagging.precision}\t${evalMetaBagging.recall}\t${evalMetaBagging.f1}")

              val stacking = new Stacking()
              val evalNaiveStacking = stacking
                .onDataSetName(dataSetName)
                .useTools(bestTools)
                .onTrainDataFrame(trainDF)
                .onTestDataFrame(testDF)
                .performEnsambleLearningOnTools(session)
              evalNaiveStacking.printResult(s"STACKING: NAIVE BEST TOOLS ON $dataSetName")


              val stackingWithMeta = new Stacking()
              val evalMetaStacking = stackingWithMeta
                .onDataSetName(dataSetName)
                .useTools(bestTools)
                .onTrainDataFrame(trainDF)
                .onTestDataFrame(testDF)
                .performEnsambleLearningOnToolsAndMetadata(session)
              evalMetaStacking.printResult(s"STACKING & METADATA: NAIVE BEST TOOLS ON $dataSetName")
              // println(s"stacking: $k\t${evalNaiveStacking.precision}\t${evalNaiveStacking.recall}\t${evalNaiveStacking.f1}\t${evalMetaStacking.precision}\t${evalMetaStacking.recall}\t${evalMetaStacking.f1}")

            })


          }
        }

      }
    }
  }


}
