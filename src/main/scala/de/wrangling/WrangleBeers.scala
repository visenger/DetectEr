package de.wrangling

import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.util.DatasetFlattener
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object WrangleBeers {

  val path = "/Users/visenger/research/datasets/craft-beers/craft-cans"
  val beersPath = s"$path/beers.csv"
  val breweriesPath = s"$path/breweries.csv"

  def main(args: Array[String]): Unit = {

    val brewery_id = "brewery_id"
    val beersSchema = Seq("tid", "abv", "ibu", "id", "beer-name", "style", brewery_id, "ounces")
    val brewerySchema = Seq(brewery_id, "brewery-name", "city", "state")

    SparkLOAN.withSparkSession("BEERS") {
      session => {

        val beersDF: DataFrame = DataSetCreator.createFrame(session, beersPath, beersSchema: _*)
        val breweriesDF: DataFrame = DataSetCreator.createFrame(session, breweriesPath, brewerySchema: _*)

        val beersBreweriesDF: DataFrame = beersDF.join(breweriesDF, Seq(brewery_id))
          .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", brewery_id, "brewery-name", "city", "state")
        beersBreweriesDF.show(67, false)

        //        beersBreweriesDF
        //          .repartition(1)
        //          .write
        //          .option("header", "true")
        //          .csv(s"$path/beers-and-breweries")

      }
    }

  }

}

object BeersDirtyMaker {
  val path = "/Users/visenger/research/datasets/craft-beers/craft-cans"
  val beersAndBreweriesPath = s"$path/beers-and-breweries.csv"
  val city = "city"
  val state = "state"
  val beersSchema = Seq("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)

  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("DIRTY-MAKER") {
      session => {

        val cleanBeersBreweriesDF: DataFrame = DataSetCreator
          .createFrame(session, beersAndBreweriesPath, beersSchema: _*)

        val Array(city_to_dirty, remains_clean_1) = cleanBeersBreweriesDF.randomSplit(Array(0.05, 0.95), 123L)
        val tmpCol = "tmp-city-location"
        val dirtyCityDF: DataFrame = city_to_dirty.withColumn(tmpCol, concat_ws(" ", col(city), col(state)))
          .drop(state)
          .drop(city)
          .withColumn(state, lit(""))
          .withColumnRenamed(tmpCol, city)
        val dirty1DF: DataFrame = dirtyCityDF
          .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)
          .union(remains_clean_1)
          .toDF(beersSchema: _*)
        dirty1DF.show(345)

        val Array(abv_to_dirty, remains_clean_2) = dirty1DF.randomSplit(Array(0.3, 0.7), 456L)
        val dirtyAbvDF: DataFrame = abv_to_dirty
          .withColumn("tmp-abv", concat(col("abv"), lit("%")))
          .drop("abv")
          .withColumnRenamed("tmp-abv", "abv")
        val dirty2DF: DataFrame = dirtyAbvDF
          .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)
          .union(remains_clean_2)
          .toDF(beersSchema: _*)
        dirty2DF.show(567)

        val Array(dirty2aDF, clean2aDF) = dirty2DF.randomSplit(Array(0.2, 0.8), seed = 56L)
        val dirty3DF: DataFrame = dirty2aDF
          .na.fill("N/A", Seq("ibu"))
          .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)
          .union(clean2aDF)
          .toDF(beersSchema: _*)
        dirty3DF.show(45)

        /**
          * adding size measures into the ounces column
          * oz.
          * oz
          * ounce
          * OZ.
          * oz. "Silo Can"
          * oz. Alumi-Tek®
          */
        val addOunce = Map(
          0 -> "oz.",
          1 -> "oz",
          2 -> "ounce",
          3 -> "OZ.",
          4 -> "oz. Silo Can",
          5 -> "oz. Alumi-Tek")


        val Array(df1, df2, df3, df4, df5, df6, cleanDF) = dirty3DF
          .randomSplit(Array(0.043, 0.028, 0.025, 0.07, 0.01, 0.06, 0.2), seed = 789L)

        val allOunces: Array[DataFrame] = Array(df1, df2, df3, df4, df5, df6)
          .zipWithIndex
          .map(entry => {
            val df: DataFrame = entry._1.toDF()
            val idx: Int = entry._2
            val ounce: String = addOunce.getOrElse(idx, "oz.")
            println(s"using ounce: $ounce")

            val withNewOuncesDF: DataFrame = df
              .withColumn("ounce-tmp", concat_ws(" ", col("ounces"), lit(ounce)))
              .drop("ounces")
              .withColumnRenamed("ounce-tmp", "ounces")
              .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)
            withNewOuncesDF
          })
        val dirtyFinalDF: DataFrame = allOunces
          .reduce((first, second) => first.union(second))
          .union(cleanDF)
          .toDF(beersSchema: _*)
        dirtyFinalDF.show(347)

        dirtyFinalDF
          .repartition(1)
          .write
          .option("header", "true")
          .csv(s"$path/dirty-beers-and-breweries-2")


      }
    }

  }
}


object DatasetFlattenerPlayground {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("flatten") {
      session => {
        Seq("beers", "flights").foreach(dataset => {
          DatasetFlattener().onDataset(dataset).flattenDirtyData(session).show()
          DatasetFlattener().onDataset(dataset).flattenCleanData(session).show()
          DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session).show()

        })
      }
    }
  }
}
