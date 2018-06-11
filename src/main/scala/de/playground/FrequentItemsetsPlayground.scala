package de.playground

import de.evaluation.f1.FullResult
import de.evaluation.util.SparkLOAN
import de.model.util.NumbersUtil
import de.util.DatasetFlattener
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql.DataFrame

object FrequentItemsetsPlayground {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("Frequent Itemsets") {
      session => {
        val sc: SparkContext = session.sparkContext

        val freqItemsets = sc.parallelize(Seq(
          new FreqItemset(Array("a"), 15L),
          new FreqItemset(Array("b"), 35L),
          new FreqItemset(Array("c", "b"), 35L),
          new FreqItemset(Array("a", "b"), 52L)
        ))

        val ar = new AssociationRules()
          .setMinConfidence(0.8)
        val results = ar.run(freqItemsets)

        results.collect().foreach { rule =>
          println(s"[${rule.antecedent.mkString(",")}=>${rule.consequent.mkString(",")} ]" +
            s" ${rule.confidence}")
        }

        val df = session.createDataFrame(Seq(
          (0, Array("a", "b", "c")),
          (1, Array("a", "b", "b", "c", "a"))
        )).toDF("id", "words")

        // fit a CountVectorizerModel from the corpus
        val cvModel: CountVectorizerModel = new CountVectorizer()
          .setInputCol("words")
          .setOutputCol("features")
          .setVocabSize(10)
          .setMinDF(2)
          .fit(df)

        // alternatively, define CountVectorizerModel with a-priori vocabulary
        //        val cvm = new CountVectorizerModel(Array("a", "b", "c"))
        //          .setInputCol("words")
        //          .setOutputCol("features")

        cvModel.transform(df).show(false)
      }
    }
  }

}

object FrequentItemsetsFromDirtyData {

  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("FrItems") {
      session => {
        val dataset: String = "beers"
        //val dirtyDataPath = "/Users/visenger/research/datasets/craft-beers/craft-cans/dirty-beers-and-breweries-2/dirty-beers-and-breweries-2.csv"
        val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
        val count: Long = dirtyDF.count()

        val aggrDirtyDF: DataFrame = dirtyDF.groupBy("attrName")
          .agg(collect_list(FullResult.value).as("column-values"))
        aggrDirtyDF.show()

        val countVectorizerModel: CountVectorizerModel = new CountVectorizer()
          .setInputCol("column-values")
          .setOutputCol("features")
          .setVocabSize(count.toInt)
          .fit(aggrDirtyDF)


        val frequentValsDF: DataFrame = countVectorizerModel.transform(aggrDirtyDF)

        frequentValsDF.printSchema()
        frequentValsDF.select("attrName", "features").show(false)

        val vocabulary: Array[String] = countVectorizerModel.vocabulary
        println(s"size of vocabulary: ${vocabulary.length}")
        val indexToValsDictionary: Map[Int, String] = vocabulary.zipWithIndex.map(_.swap).toMap

        val extract_counts = udf {
          features: org.apache.spark.ml.linalg.Vector => {

            val sparse: SparseVector = features.toSparse
            val totalVals: Int = sparse.size
            val indices: Array[Int] = sparse.indices
            val values: Array[Double] = sparse.values
            val probOfOneElement: Double = 1.0/totalVals
            val tuples: Map[String, Double] = indices.zip(values)
              .map(v => (indexToValsDictionary(v._1), NumbersUtil.round(v._2 / totalVals, 4)))
              .toMap
            tuples
          }
        }

        frequentValsDF
          .withColumn("new-features", extract_counts(frequentValsDF("features")))
          .select("attrName", "new-features")
          .show(false)


      }
    }
  }
}
