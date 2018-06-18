package de.playground

import de.evaluation.f1.FullResult
import de.evaluation.util.SparkLOAN
import de.util.DatasetFlattener
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.{AssociationRules, PrefixSpan}
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
          .setMinConfidence(0.5)
        val results = ar.run(freqItemsets)

        results.collect().foreach { rule: AssociationRules.Rule[String] =>
          println(s"[${rule.antecedent.mkString(",")}=>${rule.consequent.mkString(",")} ]" +
            s" ${rule.confidence}")
        }

        /**
          * input:
          * <(12)3>
          * <1(32)(12)>
          * <(12)5>
          * <6>
          *
          * output:
          * [[2]], 3
          * [[3]], 2
          * [[1]], 3
          * [[2, 1]], 3
          * [[1], [3]], 2
          */

        val sequences = sc.parallelize(Seq(
          Array(Array(1, 2), Array(3)),
          Array(Array(1), Array(3, 2), Array(1, 2)),
          Array(Array(1, 2), Array(5)),
          Array(Array(6))
        ), 2).cache()
        val prefixSpan = new PrefixSpan()
          .setMinSupport(0.5)
          .setMaxPatternLength(5)
        val model = prefixSpan.run(sequences)
        model.freqSequences.collect().foreach { freqSequence =>
          println(
            s"${freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]")}," +
              s" ${freqSequence.freq}")
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
        Seq("beers", "flights").foreach(dataset => {

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
          frequentValsDF.select("attrName", "features").show()

          val vocabulary: Array[String] = countVectorizerModel.vocabulary
          println(s"size of vocabulary: ${vocabulary.length}")
          val indexToValsDictionary: Map[Int, String] = vocabulary.zipWithIndex.map(_.swap).toMap

          def extract_counts = udf {
            features: org.apache.spark.ml.linalg.Vector => {

              val sparse: SparseVector = features.toSparse
              val totalVals: Int = sparse.size
              val indices: Array[Int] = sparse.indices
              val values: Array[Double] = sparse.values
              val tuples: Seq[(String, Int)] = indices.zip(values)
                // .filter(_._2 > 1.0)
                .sortWith(_._2 > _._2)
                // .map(v => (indexToValsDictionary(v._1), NumbersUtil.round(v._2 / totalVals, 4)))
                .map(v => (indexToValsDictionary(v._1), v._2.toInt))
                .toSeq


              //            val sortedTuples: mutable.LinkedHashMap[String, Double] =
              //              mutable.LinkedHashMap(tuples.toSeq.sortWith(_._2 > _._2): _*)

              tuples
            }
          }

          val withNewFeatures: DataFrame = frequentValsDF
            .withColumn("new-features", extract_counts(frequentValsDF("features")))
            .select("attrName", "new-features")

          withNewFeatures
            .show()
          withNewFeatures.printSchema()


        })


      }
    }
  }
}
