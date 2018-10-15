package de.playground

import breeze.stats._
import com.typesafe.config.{Config, ConfigFactory}
import de.error.detection.from.metadata.MisspellingErrorDetector.{allMetadataByName, allSchemasByName}
import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.{BeersSchema, Schema}
import de.evaluation.data.util.LookupColumns
import de.evaluation.util.SparkLOAN
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.prediction.CountsTableRow
import de.model.util.NumbersUtil
import de.util.DatasetFlattener
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

  Seq("fl 32082").foreach(s => {
    print(s)
    println(s.matches("^([\\d]{5}|[\\d]{9})$"))
  })

  Seq("OR", "IN", "CA", "alabama").foreach(s => {
    val state0 = "^(or|ca|in)$"
    val state1 = "^(al|ak|az|ar|ca|co|ct|de|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|mo|mt|ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|wa|wv|wi|wy|pr|dc|vi)$"
    val state2 = "^(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming|puerto rico|district of columbia|virgin islands)$"

    val value: String = s.toLowerCase
    println(value)
    println(s.toLowerCase().matches(state1))
    println(s.toLowerCase().matches(state2))
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

object SeqGroupByPlayground extends App {
  val lengths = Seq(24, 17, 19, 15, 18, 17, 20, 20, 14, 22, 20, 27, 17, 18, 12, 15, 21, 18, 21, 15, 25,
    22, 25, 18, 13, 25, 27, 12, 14, 22, 11, 15, 15, 15, 18, 16, 17, 13, 15, 22, 17, 18, 17, 15, 18, 22,
    15, 31, 18, 16, 18, 15, 19, 20, 15, 27, 17, 17, 30, 17, 27, 12, 17, 22, 31, 15, 20, 15, 15, 18, 16,
    14, 26)
  val lengthDistr: Map[Int, Int] = lengths.groupBy(length => length)
    .map(pair => (pair._1, pair._2.size))
    .toMap
  println(lengthDistr)
}

object MuseumsLoaderPlayground {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("mixed csv") {
      session => {
        //        val commonPath = "/Users/visenger/research/datasets/museum"
        //        val path = s"$commonPath/test-museum.csv"
        //        val schemaM = MuseumSchema.getSchema //Seq("Object Number", "Is Highlight", "Is Public Domain", "Object ID", "Department", "Object Name", "Title", "Culture", "Period", "Dynasty", "Reign", "Portfolio", "Artist Role", "Artist Prefix", "Artist Display Name", "Artist Display Bio", "Artist Suffix", "Artist Alpha Sort", "Artist Nationality", "Artist Begin Date", "Artist End Date", "Object Date", "Object Begin Date", "Object End Date", "Medium", "Dimensions", "Credit Line", "Geography Type", "City", "State", "County", "Country", "Region", "Subregion", "Locale", "Locus", "Excavation", "River", "Classification", "Rights and Reproduction", "Link Resource", "Metadata Date", "Repository")
        //        val museum: DataFrame = DataSetCreator.createFrame(session, path, schemaM: _*)
        //
        //        museum.printSchema()
        //        museum.show(false)


        val dataset = "museum"
        println(s"processing $dataset.....")

        val schema: Schema = allSchemasByName.getOrElse(dataset, BeersSchema)

        val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
        val creator = MetadataCreator()

        val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
        val fullMetadataDF: DataFrame = creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
        fullMetadataDF.show(50)

        val flatWithLabelDF: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)
        flatWithLabelDF.show(50, false)

        val flatWithMetadataDF: DataFrame = flatWithLabelDF.join(fullMetadataDF, Seq("attrName"))
        flatWithMetadataDF.show(50)


      }
    }
  }
}

object QuotesPlayground {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("quotes") {
      session => {
        val path = "/Users/visenger/Downloads/to-remove/test-museum.csv"

        val csv = session
          .read
          .option("header", "true")
          .csv(path)

        csv.show(false)

        val commonPath = "/Users/visenger/research/datasets/museum"
        val path2 = s"$commonPath/museum-clean-tmp.csv"
        val fullCSV: DataFrame = session
          .read
          .option("header", "true")
          .csv(path2)
        fullCSV.show(false)



        //csv.toDF(MuseumSchema.getSchema: _*).show()
      }
    }
  }
}

object DescriptiveStatPlayground extends ExperimentsCommonConfig {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("descriptive stat") {
      session => {
        val dataset = "museum"
        //    val dataset = "beers"
        val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
        val creator = MetadataCreator()

        val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
        val fullMetadataDF: DataFrame = creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
        fullMetadataDF.show(50)
        fullMetadataDF.printSchema()

        //
        //        def calc_mean_var = udf {
        //          valuesLength: mutable.Seq[Int] => {
        //            val valsAsDouble: mutable.Seq[Double] = valuesLength.map(_.toDouble)
        //            val values: DenseVector[Double] = DenseVector[Double](valsAsDouble: _*)
        //            //mean(values)
        //            //mode returns a tuple: [22.0,209] or [NaN,0] -> when values are empty
        //
        //            val mostFrequentPatternLength: Double = mode(values).mode
        //
        //            val mv = meanAndVariance(values)
        //
        //            val lowerQuartile: Double = computeQuartile(valsAsDouble, 0.25)
        //            val upperQuartile: Double = computeQuartile(valsAsDouble, 0.75)
        //
        //            Map("mean" -> mv.mean,
        //              "var" -> mv.variance,
        //              "stddev" -> mv.stdDev,
        //              "most-freq-pattern-length" -> mostFrequentPatternLength,
        //              "lower-quartile" -> lowerQuartile,
        //              "upper-quartile" -> upperQuartile)
        //          }
        //        }
        //
        //        val statCol = "all-statistics"
        //        val newMetaDF: DataFrame = fullMetadataDF
        //          .withColumn(statCol, calc_mean_var(col("pattern-length")))
        //          .withColumn("mean-pattern-length", col(statCol).getItem("mean"))
        //          .withColumn("variance-pattern-length", col(statCol).getItem("var"))
        //          .withColumn("std-dev-pattern-length", col(statCol).getItem("stddev"))
        //          .withColumn("most-freq-pattern-lenght", col(statCol).getItem("most-freq-pattern-length"))
        //          .withColumn("lower-quartile-pattern-length", col(statCol).getItem("lower-quartile"))
        //          .withColumn("upper-quartile-pattern-length", col(statCol).getItem("upper-quartile"))
        //          .drop(statCol)
        //        newMetaDF
        //          .show(50)
        //        newMetaDF.printSchema()


      }
    }
  }


  private def computeQuartile(valsAsDouble: mutable.Seq[Double], p: Double): Double = {
    var result: Double = 0.0
    if (!valsAsDouble.isEmpty) {

      result = DescriptiveStats.percentile(valsAsDouble.toArray, p)
    }
    result
  }
}

object BreezePlayground extends App {

  import breeze.linalg.{DenseVector, _}
  import breeze.stats._


  val values: Vector[Double] = DenseVector[Double](2.0, 3.0, 4.0, 5.0, 2.0, 7.0, 9.0, 7.0)
  val values_empty: Vector[Double] = DenseVector[Double]()

  // val a = DenseVector(2, 10, 3) // cause compiling error
  println((mean(values)))
  println(DescriptiveStats.percentile(values.toArray, 0.25))
  //println(DescriptiveStats.percentile(values_empty.toArray, 0.25))

  println(bincount(DenseVector[Int](0, 1, 2, 3, 1, 3, 3, 3)))

  val valsAsInt: Seq[Int] = Seq()

  if (valsAsInt.nonEmpty) println(median(valsAsInt))


}




