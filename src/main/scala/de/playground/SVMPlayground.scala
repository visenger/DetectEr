package de.playground

import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by visenger on 18/03/17.
  */
class SVMPlayground {

}

object SVMPlaygroundRunner extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("SVM") {
      session => {
        import session.implicits._

        val trainDF = DataSetCreator.createFrame(session, blackoakTrainFile, FullResult.schema: _*)
        val testDF = DataSetCreator.createFrame(session, blackoakTestFile, FullResult.schema: _*)

        val trainLibsvm: RDD[LabeledPoint] = prepareDataToLIBSVM(session, trainDF)
        val testLibsvm: RDD[LabeledPoint] = prepareDataToLIBSVM(session, testDF)

        val model: SVMModel = SVMWithSGD.train(trainLibsvm, 100)

        val modelsThreshold = model.getThreshold.get
        println(s"models threshold: ${modelsThreshold}")

        model.clearThreshold()
        //model.setThreshold(0.0005)

        val predictAndLabels: RDD[(Double, Double)] = testLibsvm.map(point => {
          val predict: Double = model.predict(point.features)
          (predict, point.label)
        })

        predictAndLabels.toDF("predicted-score", "label").distinct().sort("predicted-score").show(200)

        // Get evaluation metrics.
        val metrics = new BinaryClassificationMetrics(predictAndLabels)


        val auROC = metrics.areaUnderROC()

        println(s"AUROC: $auROC")

        metrics.pr().toDF("recall", "precision")

        val thresholdCol = "threshold"
        val f1ByThreshold = metrics.fMeasureByThreshold().toDF(thresholdCol, "F-Measure")
        val precisionByThreshold = metrics.precisionByThreshold().toDF(thresholdCol, "precision")
        val recallByThreshold = metrics.recallByThreshold().toDF(thresholdCol, "recall")

        val hospMaxPrecision = experimentsConf.getDouble(s"blackoak.max.precision")
        val hospMaxRecall = experimentsConf.getDouble(s"blackoak.max.recall")

        precisionByThreshold
          .join(recallByThreshold, thresholdCol)
          .join(f1ByThreshold, thresholdCol)
          //          .where(precisionByThreshold("precision") <= hospMaxPrecision)
          //          .where(recallByThreshold("recall") <= hospMaxRecall)
          .sort(f1ByThreshold(thresholdCol))
          .show(200)

        println(s"intercept: ${model.intercept}")
        println(s"""weights: ${model.weights.toArray.mkString(", ")}""")


      }
    }
  }

  private def prepareDataToLIBSVM(session: SparkSession, dataDF: DataFrame): RDD[LabeledPoint] = {
    import session.implicits._

    val labelAndTools = dataDF.select(FullResult.label, FullResult.tools: _*)

    val data: RDD[LabeledPoint] = labelAndTools.map(row => {
      val label: Double = row.get(0).toString.toDouble
      val toolsVals: Array[Double] = (1 until row.size)
        .map(idx => row.getString(idx).toDouble).toArray
      val features = org.apache.spark.mllib.linalg.Vectors.dense(toolsVals)
      LabeledPoint(label, features)
    }).rdd

    data
  }
}
