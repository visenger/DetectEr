package de.playground

import de.evaluation.f1.{F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.FormatUtil
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.rdd.RDD

/**
  * Created by visenger on 28/03/17.
  */
class NeuralNetworkClassifier {

}

object NeuralNetworkClassifierRunner extends ExperimentsCommonConfig {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("NEURAL-NETWORKS") {
      session => {

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        val testDF = DataSetCreator.createFrame(session, extBlackoakTestFile, FullResult.schema: _*)

        val trainLibsvm = FormatUtil.prepareDataToLIBSVM(session, trainDF, FullResult.tools)
        val testLibsvm = FormatUtil.prepareDataToLIBSVM(session, testDF, FullResult.tools)

        // specify layers for the neural network:
        // input layer of size 4 (features), two intermediate of size 5 and 4
        // and output of size 3 (classes)
        //val layers = Array[Int](4, 5, 4, 3)

        import org.apache.spark.sql.functions._

        val testDfWithRowId = testLibsvm.withColumn("row-id", monotonically_increasing_id())

        val layers = Array[Int](8, 9, 8, 2)
        // create the trainer and set its parameters
        val trainer = new MultilayerPerceptronClassifier()
          .setLayers(layers)
          .setBlockSize(128)
          .setSeed(1234L)
          .setMaxIter(100)
        val model = trainer.fit(trainLibsvm)
        val result = model.transform(testDfWithRowId)
        val predictionAndLabels = result.select("prediction", "label")
        val evaluator = new MulticlassClassificationEvaluator()
          .setMetricName("accuracy")

        println("Test set accuracy = " + evaluator.evaluate(predictionAndLabels))

        val predictAndLabel: RDD[(Double, Double)] = predictionAndLabels.rdd.map(row => {
          val prediction = row.getDouble(0)
          val label = row.getDouble(1)
          (prediction, label)
        })
        val eval = F1.evalPredictionAndLabels(predictAndLabel)
        eval.printResult("eval neural networks")

      }
    }
  }

}
