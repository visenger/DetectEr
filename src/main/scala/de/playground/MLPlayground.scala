package de.playground

/**
  * Created by visenger on 26/12/16.
  */
class MLPlayground {

}

//spark example: https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ml/EstimatorTransformerParamExample.scala
import de.evaluation.util.SparkSessionCreator
import de.model.util.AbstractParams
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.{Pipeline, PipelineStage, Transformer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.GeneralizedLinearRegression
import org.apache.spark.mllib.evaluation.{RegressionMetrics}
import org.apache.spark.mllib.util.MLUtils
import scala.language.reflectiveCalls
import scopt.OptionParser
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}


object EstimatorTransformerParamExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSessionCreator.createSession("LOGREGR")


    // Prepare training data from a list of (label, features) tuples.
    val training = sparkSession.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")

    // Create a LogisticRegression instance. This instance is an Estimator.
    val lr = new LogisticRegression()
    // Print out the parameters, documentation, and any default values.
    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

    // We may set parameters using setter methods.
    lr.setMaxIter(10)
      .setRegParam(0.01)

    // Learn a LogisticRegression model. This uses the parameters stored in lr.
    val model1 = lr.fit(training)
    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
    // we can view the parameters it used during fit().
    // This prints the parameter (name: value) pairs, where names are unique IDs for this
    // LogisticRegression instance.
    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

    // We may alternatively specify parameters using a ParamMap,
    // which supports several methods for specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter, 30) // Specify 1 Param. This overwrites the original maxIter.
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55) // Specify multiple Params.

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(training, paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

    // Prepare test data.
    val test = sparkSession.createDataFrame(Seq(
      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    )).toDF("label", "features")

    // Make predictions on test data using the Transformer.transform() method.
    // LogisticRegression.transform will only use the 'features' column.
    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
    model2.transform(test)
      .select("features", "label", "myProbability", "prediction")
      .collect()
      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
      }

    sparkSession.stop()
  }
}


object PipelineExample {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSessionCreator.createSession("PIPELINE")

    // Prepare training documents from a list of (id, text, label) tuples.
    val training = sparkSession.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "a b c d e", 0.0),
      (2L, "b d test sprark", 1.0),
      (3L, "spark f g h", 1.0),
      (4L, "hadoop mapreduce a b c d e", 0.0)
    )).toDF("id", "text", "label")

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))

    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)

    // Now we can optionally save the fitted pipeline to disk
    model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
    val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = sparkSession.createDataFrame(Seq(
      (4L, "spark i j k many"),
      (5L, "l m spark n "),
      (6L, "apache a b c d e lib"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents.
    val predictionModel = sameModel.transform(test)
    predictionModel.schema.fields.foreach(println)
    predictionModel
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
    // $example off$

    sparkSession.stop()
  }
}

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector


object Word2VecExample {
  def main(args: Array[String]) {
    val sparkSession = SparkSessionCreator.createSession("W2V")

    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sparkSession.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat great excellent".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(5)
      .setMinCount(1)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n")
    }

    val synonyms: DataFrame = model.findSynonyms("neat", 2)
    synonyms.show(2)

    val testDF = sparkSession.createDataFrame(Seq(
      "Hi I heard about Spark and try it out".split(" "),
      "Logistic regression models are cool".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    val predicting = model.transform(testDF)

    val fields: Seq[String] = predicting.schema.fieldNames.toSeq
    predicting
      .select(fields.head, fields.tail: _*)
      .collect()
      .foreach(row => println(row.mkString(", ")))

    sparkSession.stop()
  }
}


/**
  * An example runner for linear regression with elastic-net (mixing L1/L2) regularization.
  *
  *
  * {{{
  * bin/run-example ml.LinearRegressionExample --regParam 0.15 --elasticNetParam 1.0 \
  *   data/mllib/sample_linear_regression_data.txt
  * }}}
  * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
  */
object LinearRegressionExample {

  case class Params(
                     input: String = null,
                     testInput: String = "",
                     dataFormat: String = "libsvm",
                     regParam: Double = 0.15,
                     elasticNetParam: Double = 0.0,
                     maxIter: Int = 150,
                     tol: Double = 1E-6,
                     fracTest: Double = 0.3) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LinearRegressionExample") {
      head("LinearRegressionExample: an example Linear Regression with Elastic-Net app.")
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[Double]("elasticNetParam")
        .text(s"ElasticNet mixing parameter. For alpha = 0, the penalty is an L2 penalty. " +
          s"For alpha = 1, it is an L1 penalty. For 0 < alpha < 1, the penalty is a combination of " +
          s"L1 and L2, default: ${defaultParams.elasticNetParam}")
        .action((x, c) => c.copy(elasticNetParam = x))
      opt[Int]("maxIter")
        .text(s"maximum number of iterations, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Double]("tol")
        .text(s"the convergence tolerance of iterations, Smaller value will lead " +
          s"to higher accuracy with the cost of more iterations, default: ${defaultParams.tol}")
        .action((x, c) => c.copy(tol = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing. If given option testInput, " +
          s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      opt[String]("testInput")
        .text(s"input path to test dataset. If given, option fracTest is ignored." +
          s" default: ${defaultParams.testInput}")
        .action((x, c) => c.copy(testInput = x))
      opt[String]("dataFormat")
        .text("data format: libsvm (default), dense (deprecated in Spark v1.1)")
        .action((x, c) => c.copy(dataFormat = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => runLinearRegression(params)
      case _ => sys.exit(1)
    }
  }

  def runLinearRegression(params: Params): Unit = {
    val spark = SparkSessionCreator.createSession("LINEAR-REGRESSION")

    println(s"LinearRegressionExample with parameters:\n$params")

    // Load training and test data and cache it.
    val (training: DataFrame, test: DataFrame) = loadDatasets(params.input,
      params.dataFormat, params.testInput, "regression", params.fracTest)

    val lir = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setRegParam(params.regParam)
      .setElasticNetParam(params.elasticNetParam)
      .setMaxIter(params.maxIter)
      .setTol(params.tol)

    // Train the model
    val startTime = System.nanoTime()
    val lirModel = lir.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    // Print the weights and intercept for linear regression.
    println(s"Weights: ${lirModel.coefficients} Intercept: ${lirModel.intercept}")

    println("Training data results:")
    evaluateRegressionModel(lirModel, training, "label")
    println("Test data results:")
    evaluateRegressionModel(lirModel, test, "label")

    spark.stop()
  }

  /** Load a dataset from the given path, using the given format */
  private def loadData(
                        spark: SparkSession,
                        path: String,
                        format: String,
                        expectedNumFeatures: Option[Int] = None): DataFrame = {
    import spark.implicits._

    format match {
      case "dense" => MLUtils.loadLabeledPoints(spark.sparkContext, path).toDF()
      case "libsvm" => expectedNumFeatures match {
        case Some(numFeatures) => spark.read.option("numFeatures", numFeatures.toString)
          .format("libsvm").load(path)
        case None => spark.read.format("libsvm").load(path)
      }
      case _ => throw new IllegalArgumentException(s"Bad data format: $format")
    }
  }

  /**
    * Load training and test data from files.
    *
    * @param input      Path to input dataset.
    * @param dataFormat "libsvm" or "dense"
    * @param testInput  Path to test dataset.
    * @param algo       Classification or Regression
    * @param fracTest   Fraction of input data to hold out for testing. Ignored if testInput given.
    * @return (training dataset, test dataset)
    */
  private def loadDatasets(
                            input: String,
                            dataFormat: String,
                            testInput: String,
                            algo: String,
                            fracTest: Double): (DataFrame, DataFrame) = {
    val spark = SparkSessionCreator.createSession("DATA")

    // Load training data
    val origExamples: DataFrame = loadData(spark, input, dataFormat)

    // Load or create test set
    val dataframes: Array[DataFrame] = if (testInput != "") {
      // Load testInput.
      val numFeatures = origExamples.first().getAs[Vector](1).size
      val origTestExamples: DataFrame =
        loadData(spark, testInput, dataFormat, Some(numFeatures))
      Array(origExamples, origTestExamples)
    } else {
      // Split input into training, test.
      origExamples.randomSplit(Array(1.0 - fracTest, fracTest), seed = 12345)
    }

    val training = dataframes(0).cache()
    val test = dataframes(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    val numFeatures = training.select("features").first().getAs[Vector](0).size
    println("Loaded data:")
    println(s"  numTraining = $numTraining, numTest = $numTest")
    println(s"  numFeatures = $numFeatures")

    (training, test)
  }

  /**
    * Evaluate the given RegressionModel on data. Print the results.
    *
    * @param model        Must fit RegressionModel abstraction
    * @param data         DataFrame with "prediction" and labelColName columns
    * @param labelColName Name of the labelCol parameter for the model
    *
    */
  private def evaluateRegressionModel(
                                       model: Transformer,
                                       data: DataFrame,
                                       labelColName: String): Unit = {
    val fullPredictions = model.transform(data).cache()
    val predictions = fullPredictions.select("prediction").rdd.map(_.getDouble(0))
    val labels = fullPredictions.select(labelColName).rdd.map(_.getDouble(0))
    val RMSE = new RegressionMetrics(predictions.zip(labels)).rootMeanSquaredError
    println(s"  Root mean squared error (RMSE): $RMSE")
  }

}

object GeneralizedLinearRegressionExample {
  def main(args: Array[String]): Unit = {
    var spark = SparkSessionCreator.createSession("GLR")
    // Load training data
    val dataset = spark.read.format("libsvm")
      .load("src/main/resources/matrix-libsvm.txt")

    val glr = new GeneralizedLinearRegression()
      .setFamily("gamma")
      .setLink("inverse")
      .setMaxIter(100)
      .setRegParam(0.15)

    val Array(training, test) = dataset.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // Fit the model
    val model = glr.fit(training)

    // Print the coefficients and intercept for generalized linear regression model
    println(s"Coefficients: ${model.coefficients}")
    println(s"Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    val summary = model.summary
    println(s"Coefficient Standard Errors: ${summary.coefficientStandardErrors.mkString(",")}")
    println(s"T Values: ${summary.tValues.mkString(",")}")
    println(s"P Values: ${summary.pValues.mkString(",")}")
    println(s"Dispersion: ${summary.dispersion}")
    println(s"Null Deviance: ${summary.nullDeviance}")
    println(s"Residual Degree Of Freedom Null: ${summary.residualDegreeOfFreedomNull}")
    println(s"Deviance: ${summary.deviance}")
    println(s"Residual Degree Of Freedom: ${summary.residualDegreeOfFreedom}")
    println(s"AIC: ${summary.aic}")
    println("Deviance Residuals: ")
    //summary.residuals().show()


    val prediction = model.transform(test)
    prediction.show(12)

    spark.stop()
  }
}

