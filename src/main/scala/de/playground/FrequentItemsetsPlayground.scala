package de.playground

import de.evaluation.util.SparkLOAN
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.{AssociationRules, PrefixSpan}

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
          .setVocabSize(3)
          .setMinDF(2)
          .fit(df)

        // alternatively, define CountVectorizerModel with a-priori vocabulary
        val cvm = new CountVectorizerModel(Array("a", "b", "c"))
          .setInputCol("words")
          .setOutputCol("features")

        cvModel.transform(df).show(false)
      }
    }
  }

}
