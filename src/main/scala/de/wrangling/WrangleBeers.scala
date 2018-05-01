package de.wrangling

import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.DataFrame

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
