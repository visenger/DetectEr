package de.playground

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.util.LookupColumns
import de.experiments.features.prediction.CountsTableRow
import de.model.util.NumbersUtil
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * Created by visenger on 16/11/16.
  */
object Playground extends App {

  println("hello world!")

  private val config: Config = ConfigFactory.load()
  private val path: String = config.getString("data.BlackOak.clean-data-path")
  println("path = " + path)

  /**
    *
    * (exists-2,-0.27)
    * (exists-3,-0.17)
    * (intercept,-0.64)
    * (trainR,0.3811)
    * (exists-5,0.76)
    * (exists-1,0.15)
    * (trainF1,0.4425)
    * (threshold,0.4176)
    * (trainP,0.5276)
    * (exists-4,-0.17)
    **/

  println("-0.27".toDouble)

}


object StatPlayground {
  def main(args: Array[String]): Unit = {

    val rows = Seq(CountsTableRow(1.0, 1.0, 103845.0, 38326.0, 38326.0, 38326.0),
      CountsTableRow(1.0, 0.0, 103845.0, 0.0, 38326.0, 65519.0),
      CountsTableRow(0.0, 1.0, 103845.0, 0.0, 65519.0, 38326.0),
      CountsTableRow(0.0, 0.0, 103845.0, 65519.0, 65519.0, 65519.0))

    println((1 to 3).foldLeft(0.0)((acc, item) => acc + item))
    //Todo:  NaN is destroying your result
    val mutualInformation0: Double = rows.foldLeft(0.0)((acc, row) => {
      val item = ((row.xyCount / row.total) * Math.log((row.total * row.xyCount) / (row.xCount * row.yCount)))
      acc + item
    })

    val mutualInformation1 = rows
      .map(row => ((row.xyCount / row.total) * Math.log((row.total * row.xyCount) / (row.xCount * row.yCount))))
      .filterNot(member => member.isNaN)
      .foldLeft(0.0)((acc, item) => acc + item)


    val mutualInformation: Double =
      ((rows(0).xyCount / rows(0).total) * Math.log((rows(0).total * rows(0).xyCount) / (rows(0).xCount * rows(0).yCount))
        + (rows(1).xyCount / rows(1).total) * Math.log((rows(1).total * rows(1).xyCount) / (rows(1).xCount * rows(1).yCount))
        + (rows(2).xyCount / rows(2).total) * Math.log((rows(2).total * rows(2).xyCount) / (rows(2).xCount * rows(2).yCount))
        + (rows(3).xyCount / rows(3).total) * Math.log((rows(3).total * rows(3).xyCount) / (rows(3).xCount * rows(3).yCount)))
    println(s"MI: $mutualInformation vs $mutualInformation0 vs $mutualInformation1")

  }
}

case class Record(key: Int, value: String)

object SparkPlayground {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("spark-sql-playground")
      .master("local[4]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import session.implicits._

    val df = Seq(("stuff2", "stuff2", null),
      ("stuff2", "stuff2", Array("value1", "value2")),
      ("stuff3", "stuff3", Array("value3")))
      .toDF("field", "field2", "values")
    df.show()


    val array_ = udf(() => Array.empty[String])

    val df2 = df.withColumn("values", coalesce(df("values"), array_()))
    df2.show()


    val data: DataFrame = Seq((0, "DO"),
      (1, "FR"),
      (0, "MK"),
      (0, "FR"),
      (0, "RU"),
      (0, "TN"),
      (0, "TN"),
      (0, "KW"),
      (1, "RU"),
      (0, "JP"),
      (0, "US"),
      (0, "CL"),
      (0, "ES"),
      (0, "KR"),
      (0, "US"),
      (0, "IT"),
      (0, "SE"),
      (0, "MX"),
      (0, "CN"),
      (1, "EE")).toDF("x", "y")
    //data.show()

    val cooccurrDF: DataFrame = data
      .groupBy(col("x"), col("y"))
      .count()
      .toDF("x", "y", "count-x-y")

    val windowX: WindowSpec = Window.partitionBy("x")
    val windowY: WindowSpec = Window.partitionBy("y")

    val countsDF: DataFrame = cooccurrDF
      .withColumn("count-x", sum("count-x-y") over windowX)
      .withColumn("count-y", sum("count-x-y") over windowY)
      .withColumn("list", collect_list("y") over windowX)
    countsDF.show()

    session.stop();

  }


}


object F1Calculator extends App {

  import de.model.util.NumbersUtil.round

  val prTuples: Seq[(Double, Double)] = Seq((0.81, 0.97))

  prTuples.foreach(tuple => {
    val f1 = 2 * (tuple._1.toDouble * tuple._2.toDouble) / (tuple._1 + tuple._2)

    println(s"Precision: ${round(tuple._1)}, Recall: ${round(tuple._2)}, F1: ${round(f1, 4)}")

  })
}

object ListsPlayground extends App {

  private val seq0 = Seq("a", "c")
  private val seq1 = Seq("a", "b")
  val seq2 = null
  private val combi = Seq(seq0, seq1, seq2).filter(_ != null).flatten
  println(combi)

  val emptySet = Seq(seq2, Seq()).filter(_ != null).flatten.toSet
  println(emptySet)

  private val newSeqOfThree: Seq[String] = seq0.take(3)


  println(newSeqOfThree)

}

object RepairPlayground extends App {

  val repair1: String = null
  val repair2: String = null
  val allClean: mutable.Seq[String] = null
  val fdClean: mutable.Seq[String] = null
  val hist: mutable.Seq[String] = null


  val totalNumberOfSets = 5
  val totalListOfRepair = Seq(Seq(repair1), Seq(repair2), allClean, fdClean, hist).filter(_ != null).flatten.filter(_ != null)
  val totalSet = totalListOfRepair.toSet

  println(totalSet)

  val valuesWithProbs: Map[String, Double] = totalSet.map(element => {
    val numSetsIncludingElement: Int = totalListOfRepair.count(_.equalsIgnoreCase(element))
    val probOfElement = NumbersUtil.round(numSetsIncludingElement / totalNumberOfSets.toDouble, 4)
    element -> probOfElement
  }).toMap

  val mostFrequentElements: Seq[(String, Double)] = valuesWithProbs.toSeq.sortWith((pair1, pair2) => pair1._2 > pair2._2)
  val initProbability: Double = NumbersUtil.round(1 / totalNumberOfSets.toDouble, 4)
  val mostFrequentRepair: Seq[(String, Double)] = mostFrequentElements.filter(el_p => el_p._2 > initProbability)
  val endValues: Seq[String] = mostFrequentRepair.map(_._1).toSeq

  print(endValues)
}

object RegexPlayground extends App {
  Seq("12.0 oz.", "12.0", "N/A", "0")
    .foreach(s => {
      print(s)
      println(s.matches("^(0|[1-9]\\d*)?(\\.\\d+)?(?<=\\d)$"))
    })
}

object ConfigPlayground extends App {

  import scala.collection.JavaConversions._

  Seq("beers", "flights").foreach(conf => {
    val datasetConf: Config = ConfigFactory.load(s"$conf.conf")
    val size: Int = datasetConf.getConfigList("lookup.columns").size()
    println(size)


    val lookupColumns: List[LookupColumns] = datasetConf
      .getConfigList("lookup.columns")
      .toList
      .map(c => {
        LookupColumns(c.getString("name"), c.getString("source"))
      })
    lookupColumns.foreach(println)
  })
}


