package de.playground

import de.evaluation.f1.{F1, FullResult}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.model.util.FormatUtil
import org.apache.spark.mllib.tree.DecisionTree

/**
  * Created by visenger on 28/03/17.
  */
class DecisionTreesPlayground {

}

object DecisionTreesPlaygroundRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("DECISION-TREES") {
      session => {
        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        val testDF = DataSetCreator.createFrame(session, extBlackoakTestFile, FullResult.schema: _*)

        val trainLibsvm = FormatUtil.prepareDataToLabeledPoints(session, trainDF, FullResult.tools)
        val testLibsvm = FormatUtil.prepareDataToLabeledPoints(session, testDF, FullResult.tools)

        // Train a DecisionTree model.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.

        /**
          * Method to train a decision tree model for binary or multiclass classification.
          *
          * input                   Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
          * Labels should take values {0, 1, ..., numClasses-1}.
          * numClasses              Number of classes for classification.
          * categoricalFeaturesInfo Map storing arity of categorical features. An entry (n -> k)
          * indicates that feature n is categorical with k categories
          * indexed from 0: {0, 1, ..., k-1}.
          * impurity                Criterion used for information gain calculation.
          * Supported values: "gini" (recommended) or "entropy".
          * maxDepth                Maximum depth of the tree (e.g. depth 0 means 1 leaf node, depth 1 means
          * 1 internal node + 2 leaf nodes).
          * (suggested value: 5)
          * maxBins                 Maximum number of bins used for splitting features.
          * (suggested value: 32)
          * DecisionTreeModel that can be used for prediction.
          */
        val numClasses = 2
        val categoricalFeaturesInfo = Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2, 6 -> 2, 7 -> 2)
        //        val impurity = "entropy"
        val impurity = "gini"
        val maxDepth = 8
        val maxBins = 32

        val model = DecisionTree.trainClassifier(trainLibsvm, numClasses, categoricalFeaturesInfo,
          impurity, maxDepth, maxBins)

        val predictionAndLabel = testLibsvm.map { point =>
          val prediction = model.predict(point.features)
          (prediction, point.label)
        }

        val eval = F1.evalPredictionAndLabels(predictionAndLabel)
        eval.printResult(s"decision tree alg based on $impurity")


      }
    }
  }
}


