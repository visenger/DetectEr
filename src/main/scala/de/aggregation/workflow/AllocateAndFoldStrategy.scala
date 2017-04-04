package de.aggregation.workflow

import java.util.Objects

import de.evaluation.f1.{Eval, F1, FullResult, GoldStandard}
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.experiments.cosine.similarity.{AllToolsSimilarity, Cosine}
import de.model.kappa.{Kappa, KappaEstimator}
import de.model.logistic.regression.LogisticRegressionCommonBase
import de.model.multiarmed.bandit.{MultiarmedBanditsExperimentBase, ToolExpectation}
import de.model.mutual.information.{PMIEstimator, ToolPMI}
import de.model.util.{FormatUtil, ModelUtil}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{array, lit, monotonically_increasing_id, udf}
import org.apache.spark.sql._

import scala.collection.immutable.Seq
import scala.collection.mutable

/**
  * Tools aggregation strategy:
  * 1.Step: Resources allocation (multiarmed bandit based tools selection)
  * 2.Step: Resources folding (tools clustering based on similarity measures)
  */
class AllocateAndFoldStrategy {

}

case class Tool(name: String) {
  override def hashCode(): Int = Objects.hashCode(name)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Tool]

  override def equals(other: scala.Any): Boolean = {

    other match {
      case other: Tool => other.canEqual(this) && other.hashCode() == this.hashCode()
      case _ => false
    }

  }

}

case class ToolsCombination(combi: List[Tool]) {
  override def hashCode(): Int = {
    combi.foldLeft(0)((acc, tool) => acc + tool.hashCode())
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ToolsCombination]

  override def equals(other: scala.Any): Boolean = {
    other match {
      case other: ToolsCombination => other.canEqual(this) && other.hashCode() == this.hashCode()
      case _ => false
    }
  }

}

case class UnionAll(precision: Double, recall: Double, f1: Double)

case class MinK(k: Int, precision: Double, recall: Double, f1: Double)

case class LinearCombi(precision: Double, recall: Double, f1: Double, functionStr: String)

//Kappa, //Cosine, //ToolPMI
case class AggregatedTools(dataset: String,
                           combi: ToolsCombination,
                           unionAll: UnionAll,
                           minK: MinK,
                           linearCombi: LinearCombi,
                           bayesCombi: LinearCombi,
                           allPMI: List[ToolPMI],
                           allCosineSimis: List[Cosine],
                           allKappas: List[Kappa]) extends ExperimentsCommonConfig {

  def makeLatexString(): String = {
    //get real tool name -> from experiments.config file
    val tools = combi.combi.map(_.name).map(tool => getName(tool))
    val num = tools.size
    val latexString =
      s"""
         |\\multirow{4}{*}{\\begin{tabular}[c]{@{}l@{}}${tools.mkString("+")}\\\\ $$ ${linearCombi.functionStr} $$ \\end{tabular}}
         |                                                                        & UnionAll & ${unionAll.precision} & ${unionAll.recall} & ${unionAll.f1}  \\\\
         |                                                                        & Min-$num    & ${minK.precision} & ${minK.recall} & ${minK.f1}  \\\\
         |                                                                        & LinComb  & ${linearCombi.precision} & ${linearCombi.recall} & ${linearCombi.f1} \\\\
         |                                                                        & NaiveBayes  & ${bayesCombi.precision} & ${bayesCombi.recall} & ${bayesCombi.f1} \\\\
         |\\midrule
         |
                 """.stripMargin
    latexString

  }
}


object AllocateAndFoldStrategyRunner extends ExperimentsCommonConfig
  with LogisticRegressionCommonBase
  with MultiarmedBanditsExperimentBase {

  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("STRATEGY-FOR-TOOLS") {
      session => {
        import org.apache.spark.sql.functions._
        import session.implicits._

        val dataSetName = "ext.blackoak"
        val maxPrecision = experimentsConf.getDouble(s"${dataSetName}.max.precision")
        val maxRecall = experimentsConf.getDouble(s"${dataSetName}.max.recall")

        val maximumF1 = experimentsConf.getDouble(s"${dataSetName}.max.F1")
        val minimumF1 = experimentsConf.getDouble(s"${dataSetName}.min.F1")

        val trainDF = DataSetCreator.createFrame(session, extBlackoakTrainFile, FullResult.schema: _*)
        val testDF = DataSetCreator.createFrame(session, extBlackoakTestFile, FullResult.schema: _*)


        val experimentsCSV = DataSetCreator.createFrame(session, multiArmedBandResults, schema: _*)
        val experimentSettings: DataFrame = getMABanditExperimentsSettings(session, experimentsCSV)

        val allDatasets: Array[String] = experimentsCSV
          .select(experimentsCSV.col("dataset"))
          .distinct()
          .map(row => row.getString(0))
          .collect()

        //going through all datasets:
        allDatasets.foreach(data => {

          val experimentsByDataset: Dataset[Row] =
            experimentSettings
              .where(experimentSettings.col("dataset") === data)

          val allCombis: Array[String] = experimentsByDataset
            .select(experimentSettings.col("toolscombi"))
            .distinct()
            .map(row => row.getString(0))
            .collect()

          val toolsCombinations: List[ToolsCombination] = allCombis.map(l => {
            val toolsAsList: List[Tool] = l.split(",").map(Tool(_)).toList
            ToolsCombination(toolsAsList)
          }).toSet.toList


          //todo: estimate max precision and recall from the baseline:

          val path = getTestDatasetPath(data)
          val fullResult: DataFrame = DataSetCreator.createFrame(session, path, FullResult.schema: _*)

          val pmiEstimator = new PMIEstimator()
          val kappaEstimator = new KappaEstimator()
          val toolsSimilarity = new AllToolsSimilarity()


          val aggregatedTools: List[AggregatedTools] = toolsCombinations.map(toolsCombination => {
            val tools: List[String] = toolsCombination.combi.map(_.name)
            val labelAndTools = fullResult.select(FullResult.label, tools: _*).cache()
            val unionAllEval: Eval = F1.evaluate(labelAndTools)

            val unionAll = UnionAll(unionAllEval.precision, unionAllEval.recall, unionAllEval.f1)

            val k = tools.size
            val minKEval = F1.evaluate(labelAndTools, k)
            val minK = MinK(k, minKEval.precision, minKEval.recall, minKEval.f1)

            val linearCombiEval: Eval = F1.evaluateLinearCombiWithLBFGS(session, data, tools)
            val linearCombi = LinearCombi(
              linearCombiEval.precision,
              linearCombiEval.recall,
              linearCombiEval.f1,
              linearCombiEval.info)

            val combiWithNaiveBayes = F1.evaluateLinearCombiWithNaiveBayes(session, data, tools)
            val naiveBayesCombi = LinearCombi(
              combiWithNaiveBayes.precision,
              combiWithNaiveBayes.recall,
              combiWithNaiveBayes.f1,
              combiWithNaiveBayes.info)


            val allMetrics: List[(ToolPMI, Kappa, Cosine)] = tools.combinations(2).map(pair => {

              val tool1 = pair(0)
              val tool2 = pair(1)

              val pmi: ToolPMI = pmiEstimator.computePMI(labelAndTools, Seq(tool1, tool2))
              val kappa: Kappa = kappaEstimator.computeKappa(labelAndTools, Seq(tool1, tool2))
              val cosine: Cosine = toolsSimilarity.computeCosine(session, labelAndTools, (tool1, tool2))

              (pmi, kappa, cosine)
            }).toList


            val allPMIs: List[ToolPMI] = allMetrics.map(_._1)
            val allKappas: List[Kappa] = allMetrics.map(_._2)
            val allCosine: List[Cosine] = allMetrics.map(_._3)


            AggregatedTools(data, toolsCombination, unionAll, minK, linearCombi, naiveBayesCombi, allPMIs, allCosine, allKappas)
          })

          //todo: perform aggregation here - all information already there!
          //println(s"data: $data -> ${toolsCombinations.size} ; ")
          //aggregatedTools.foreach(println)

          println(s"$data BEST combination:")

          val topCombinations: List[AggregatedTools] = aggregatedTools
            .sortWith((t1, t2)
            => t1.minK.precision >= t2.minK.precision
                && t1.unionAll.recall >= t2.unionAll.recall
                && t1.linearCombi.f1 >= t2.linearCombi.f1
              //  && t1.bayesCombi.f1 >= t2.bayesCombi.f1
            )


          //          topCombinations.filter(_.combi.combi.size > 2).foreach(combi => {
          //            val latexString = combi.makeLatexString()
          //            println(latexString)
          //
          //          })

          val topCombi = topCombinations.filter(_.combi.combi.size > 3).head
          val bestTools: Seq[String] = topCombi.combi.combi.map(_.name)

          println(s"top tools to consider: ${bestTools.map(getExtName(_)).mkString(",")}")


          /* TODO: the following code is duplication from the class de.experiments.clustering.ClusterAndCombineStrategy*/

          //START ensemble learning on best tools:
          val trainLabeledPointsTools = FormatUtil
            .prepareDataToLabeledPoints(session, trainDF, bestTools)

          val bayesModel = NaiveBayes.train(trainLabeledPointsTools, lambda = 1.0, modelType = "bernoulli")
          val (bestLogRegrData, bestLogRegModel) = ModelUtil.getBestModel(maxPrecision, maxRecall, trainLabeledPointsTools, trainLabeledPointsTools)

          //start: decision tree
          val numClasses = 2
          val maxDepth = bestTools.length
          /*8*/
          val categoricalFeaturesInfo: Map[Int, Int] = (0 until maxDepth).map(attr => attr -> numClasses).toMap
          //Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2, 6 -> 2, 7 -> 2)
          //        val impurity = "entropy"
          val impurity = "gini"

          val maxBins = 32

          val decisionTreeModel = DecisionTree.trainClassifier(trainLabeledPointsTools, numClasses, categoricalFeaturesInfo,
            impurity, maxDepth, maxBins)
          //finish: decision tree
          val predictByDT = udf { features: org.apache.spark.mllib.linalg.Vector => decisionTreeModel.predict(features) }

          val predictByLogRegr = udf { features: org.apache.spark.mllib.linalg.Vector => {
            bestLogRegModel.setThreshold(bestLogRegrData.bestThreshold)
            bestLogRegModel.predict(features)
          }
          }
          val predictByBayes = udf { features: org.apache.spark.mllib.linalg.Vector => bayesModel.predict(features) }
          val minKTools = udf { (k: Int, tools: org.apache.spark.mllib.linalg.Vector) => {
            val sum = tools.toArray.sum
            val errorDecision = if (sum >= k) 1.0 else 0.0
            errorDecision
          }
          }

          val unionall = "unionAll"
          val features = "features"
          val rowId = "row-id"
          val minKCol = "minK"

          val testLabeledPointsTools: DataFrame = FormatUtil
            .prepareDataToLabeledPoints(session, testDF, bestTools)
            .toDF(FullResult.label, features)
            .withColumn(rowId, monotonically_increasing_id())

          val naiveBayesCol = "naive-bayes"
          val dtCol = "decision-tree"
          val logRegrCol = "log-regr"
          val predictCol = "prediction"


          val toolsNumber = bestTools.length
          var allClassifiers = testLabeledPointsTools
            .withColumn(dtCol, predictByDT(testLabeledPointsTools(features)))
            .withColumn(naiveBayesCol, predictByBayes(testLabeledPointsTools(features)))
            .withColumn(logRegrCol, predictByLogRegr(testLabeledPointsTools(features)))
            .withColumn(unionall, minKTools(lit(1), testLabeledPointsTools(features)))
            .withColumn(minKCol, minKTools(lit(toolsNumber), testLabeledPointsTools(features)))
            .select(rowId, FullResult.label, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol)
            .toDF()

          //start:neural networks

          val nextLayer = toolsNumber + 1
          val layers = Array[Int](toolsNumber, nextLayer, toolsNumber, numClasses)
          val trainer = new MultilayerPerceptronClassifier()
            .setLayers(layers)
            .setBlockSize(128)
            .setSeed(1234L)
            .setMaxIter(100)
          val nnTrain = FormatUtil.prepareDataToLIBSVM(session, trainDF, bestTools)
          val networkModel = trainer.fit(nnTrain)
          val testWithRowId = testDF.withColumn(rowId, monotonically_increasing_id())
          val nnTest = FormatUtil.prepareDataWithRowIdToLIBSVM(session, testWithRowId, bestTools)
          val result = networkModel.transform(nnTest)
          val nnPrediction = result.select(rowId, predictCol)
          //end:neural networks

          val nNetworksCol = "n-networks"
          allClassifiers = allClassifiers
            .join(nnPrediction, rowId)
            .withColumnRenamed(predictCol, nNetworksCol)
            .select(rowId, FullResult.label, nNetworksCol, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol)


          //all possible combinations of errors classification and their counts
          val allResults: RDD[(Double, Double, Double, Double, Double, Double, Double)] = allClassifiers
            .rdd
            .map(row => {
              //val rowId = row.getLong(0)
              val label = row.getDouble(1)
              val nn = row.getDouble(2)
              val dt = row.getDouble(3)
              val bayes = row.getDouble(4)
              val logregr = row.getDouble(5)
              val unionAll = row.getDouble(6)
              val mink = row.getDouble(7)
              (label, nn, dt, bayes, logregr, unionAll, mink)
            })

          val countByValue = allResults.countByValue()

          println(s"label, nn, dt, naive bayes, log regression, unionall, min-$toolsNumber : count")
          countByValue.foreach(entry => {
            println(s"(${entry._1._1.toInt}, ${entry._1._2.toInt}, ${entry._1._3.toInt}, ${entry._1._4.toInt}, ${entry._1._5.toInt}, ${entry._1._6.toInt}, ${entry._1._7.toInt}) : ${entry._2}")
          })

          //F1 for all classifiers performed on tools result:
          val predAndLabelNN = FormatUtil.getPredictionAndLabel(allClassifiers, nNetworksCol)
          val evalNN = F1.evalPredictionAndLabels(predAndLabelNN)
          evalNN.printResult("Neural Networks")

          val predictionAndLabelDT = FormatUtil.getPredictionAndLabel(allClassifiers, dtCol)
          val evalDTrees = F1.evalPredictionAndLabels(predictionAndLabelDT)
          evalDTrees.printResult("decision trees")

          val predAndLabelLogReg = FormatUtil.getPredictionAndLabel(allClassifiers, logRegrCol)
          val evalLogReg = F1.evalPredictionAndLabels(predAndLabelLogReg)
          evalLogReg.printResult("logistic regression")

          val predictionAndLabelNB = FormatUtil.getPredictionAndLabel(allClassifiers, naiveBayesCol)
          val evalNB = F1.evalPredictionAndLabels(predictionAndLabelNB)
          evalNB.printResult("naive bayes")


          //Now combine all classifiers:
          println(s"META: COMBINE CLASSIFIERS")

          val minK = udf { (k: Int, tools: mutable.WrappedArray[Double]) => {
            val sum = tools.count(_ == 1.0)
            val errorDecision = if (sum >= k) 1.0 else 0.0
            errorDecision
          }
          }

          val clColumns: Array[Column] = Array(
            allClassifiers(nNetworksCol),
            allClassifiers(dtCol),
            allClassifiers(naiveBayesCol),
            allClassifiers(logRegrCol),
            allClassifiers(unionall),
            allClassifiers(minKCol))

          val finalCombiCol = "final-combi"
          (1 to clColumns.length).foreach(k => {
            val finalResult: DataFrame =
              allClassifiers
                .withColumn(finalCombiCol, minK(lit(k), array(clColumns: _*)))
                .select(FullResult.label, finalCombiCol)
                .toDF()

            val predictionAndLabel = finalResult.rdd.map(row => {
              val label = row.getDouble(0)
              val prediction = row.getDouble(1)
              (prediction, label)
            })

            val eval = F1.evalPredictionAndLabels(predictionAndLabel)
            eval.printResult(s"min-$k combination of nn, dt, nb, logreg, unionall, min-$toolsNumber:")
          })

          //todo: Majority wins
          val majorityVoter = udf { (tools: mutable.WrappedArray[Double]) => {
            val total = tools.length
            val sum1 = tools.count(_ == 1.0)
            var sum0 = total - sum1
            val errorDecision = if (sum1 >= sum0) 1.0 else 0.0
            errorDecision
          }
          }

          val majorityVoterCol = "majority-voter"
          val majorityVoterStrategy = allClassifiers
            .withColumn(majorityVoterCol, majorityVoter(array(clColumns: _*)))
            .select(FullResult.label, majorityVoterCol)
            .toDF()

          val majorityVotersPredAndLabels = FormatUtil.getPredictionAndLabel(majorityVoterStrategy, majorityVoterCol)
          val majorityEval = F1.evalPredictionAndLabels(majorityVotersPredAndLabels)
          majorityEval.printResult("majority voters")
          //end: Majority wins

          val allClassifiersCols = Array(nNetworksCol, dtCol, naiveBayesCol, logRegrCol, unionall, minKCol).toSeq

          val labeledPointsClassifiers = FormatUtil.prepareDoublesToLabeledPoints(session, allClassifiers, allClassifiersCols)

          val Array(trainClassifiers, testClassifiers) = labeledPointsClassifiers.randomSplit(Array(0.2, 0.8))

          //Logistic Regression for classifier combination:
          val (classiBestModelData, classiBestModel) =
          //todo: Achtung: we removed the max precision and max recall threshold
            ModelUtil.getBestModel(0.99, 0.99, trainClassifiers, trainClassifiers)

          val predictByLogRegrCombi = udf { features: org.apache.spark.mllib.linalg.Vector => {
            classiBestModel.setThreshold(classiBestModelData.bestThreshold)
            classiBestModel.predict(features)
          }
          }

          val testClassifiersDF = testClassifiers.toDF()

          val logRegrOfAllCombi: RDD[(Double, Double)] = testClassifiersDF
            .withColumn("lr-of-combi", predictByLogRegrCombi(testClassifiersDF(features)))
            .select(FullResult.label, "lr-of-combi")
            .rdd.map(row => {
            val label = row.getDouble(0)
            val prediction = row.getDouble(1)
            (prediction, label)
          })

          val evalCombi = F1.evalPredictionAndLabels(logRegrOfAllCombi)
          evalCombi.printResult("linear combi of all")

          //Naive Bayes for classifier combination:
          val naiveBayesModel = NaiveBayes.train(trainClassifiers, lambda = 1.0, modelType = "bernoulli")
          val predictByNaiveBayes = udf { features: org.apache.spark.mllib.linalg.Vector => {
            naiveBayesModel.predict(features)
          }
          }

          val bayesOfAllCombi: RDD[(Double, Double)] = testClassifiersDF
            .withColumn("bayes-of-combi", predictByNaiveBayes(testClassifiersDF(features)))
            .select(FullResult.label, "bayes-of-combi")
            .rdd.map(row => {
            val label = row.getDouble(0)
            val prediction = row.getDouble(1)
            (prediction, label)
          })


          val evalCombiBayes = F1.evalPredictionAndLabels(bayesOfAllCombi)
          evalCombiBayes.printResult("bayes combi of all")

          //DecisionTree for classifier combi;
          val toolsFeaturesInfo = Map[Int, Int](0 -> 2, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2, 5 -> 2)
          // val impurityFinal = "entropy"
          val mDepth = 6

          val model = DecisionTree.trainClassifier(trainClassifiers, numClasses, toolsFeaturesInfo,
            impurity, mDepth, maxBins)

          val predictionAndLabel = testClassifiers.map { point =>
            val prediction = model.predict(point.features)
            (prediction, point.label)
          }

          val evalDT = F1.evalPredictionAndLabels(predictionAndLabel)
          evalDT.printResult(s"decision tree combi based on $impurity")



          /* the  code above is duplication*/

        })
      }
    }
  }

  private def getMABanditExperimentsSettings(session: SparkSession, experimentsCSV: DataFrame): DataFrame = {
    import session.implicits._
    val byDataset = experimentsCSV.groupByKey(row => row.getAs[String]("dataset"))

    val allData: Dataset[(String, String, String)] = byDataset.flatMapGroups((dataset, bandits) => {
      //pro dataset
      val algorithmsAndToolsCombi: Iterator[(String, String)] = bandits.flatMap(banditAlg => {
        //pro algorithm on dataset:
        val banditRow: Map[String, String] = banditAlg.getValuesMap[String](schema)
        val expectations = banditRow.getOrElse("expectations", "")
        //val Array(a, b, c, d, e) = expectations.split("\\|")
        //1:0.4143 -> toolId:expectation
        val resultOfBanditRun: Seq[ToolExpectation] =
        expectations.split("\\|").toList
          .map(e => new ToolExpectation().apply(e))

        val sortedResults = resultOfBanditRun
          .sortWith((t1, t2) => t1.expectation > t2.expectation)

        val toolsCombiByAlgorithm: Seq[String] =
          (2 until sortedResults.size)
            .map(toolsNum => {
              //top N tools:
              val whatToolsToEval: Seq[ToolExpectation] = sortedResults.take(toolsNum)
              val selectTools = whatToolsToEval.map(t => s"${GoldStandard.exists}-${t.id}").mkString(",")
              selectTools
            })
        val algorithm = banditRow.getOrElse("banditalg", "")
        val algorithmToTools = toolsCombiByAlgorithm.map(tools => (algorithm, tools))
        algorithmToTools
      })
      val dataToAlgorithmToTools = algorithmsAndToolsCombi.map(e => {
        (dataset, e._1, e._2)
      })
      dataToAlgorithmToTools
    })

    val experimentSettings: DataFrame = allData.toDF("dataset", "banditalg", "toolscombi")
    experimentSettings
  }
}
