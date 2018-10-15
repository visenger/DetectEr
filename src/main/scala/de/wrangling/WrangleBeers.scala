package de.wrangling

import de.evaluation.data.metadata.MetadataCreator
import de.evaluation.data.schema.BeersSchema
import de.evaluation.data.util.WriterUtil
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.ExperimentsCommonConfig
import de.util.DatasetFlattener
import de.wrangling.BeersDirtyMaker.{city, state}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

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
  val beersSchema = BeersSchema.getSchema // Seq("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)

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

        //        dirtyFinalDF
        //          .repartition(1)
        //          .write
        //          .option("header", "true")
        //          .csv(s"$path/dirty-beers-and-breweries-2")

        WriterUtil.persistCSV(dirtyFinalDF, s"$path/dirty-beers-and-breweries-2")


      }
    }

  }
}


object DatasetFlattenerPlayground {
  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("flatten") {
      session => {
        Seq("beers_dirty_5_explicitmissingvalue",
          "beers_dirty_5_implicitmissingvaluemedianmode",
          "beers_dirty_5_noise",
          "beers_dirty_5_randomactivedomain",
          "beers_dirty_5_similarbasedactivedomain",
          "beers_dirty_5_typoGenerator"
        ).foreach(dataset => {
          println(s"processing dataset: $dataset")

          val dirtyData: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
          dirtyData.where(col("tid") === "0").show()
          dirtyData.groupBy(col("tid")).count().where(col("count") > 11).show()

          val cleanData: DataFrame = DatasetFlattener().onDataset(dataset).flattenCleanData(session)
          cleanData.show()

          val flatDiff: DataFrame = DatasetFlattener().onDataset(dataset).makeFlattenedDiff(session)
          flatDiff
            .where(col("label") === "1")
            .show(50)

        })
      }
    }
  }
}


object BeersMetadataExplorer extends ExperimentsCommonConfig {

  def main(args: Array[String]): Unit = {


    SparkLOAN.withSparkSession("exploring beers") {
      session => {
        Seq("beers").foreach(dataset => {
          println(s"processing $dataset.....")

          val dataPath = allRawData.getOrElse(dataset, "unknown")

          val dirtyOriginDF: DataFrame = DataSetCreator.createFrame(session, dataPath, BeersSchema.getSchema: _*)
          dirtyOriginDF.show(false)

          dirtyOriginDF
            .groupBy(col("style")).count()
            .where(col("style").contains("IPA"))
            .show(100, false)

          val metadataPath: String = allMetadataByName.getOrElse(dataset, "unknown")
          val creator = MetadataCreator()

          val dirtyDF: DataFrame = DatasetFlattener().onDataset(dataset).flattenDirtyData(session)
          dirtyDF.show()
          val fullMetadataDF: DataFrame = creator.getMetadataWithCounts(session, metadataPath, dirtyDF)
          fullMetadataDF.show()
        })
      }
    }
  }

}


object BeersMissfieldedValsInjector extends ExperimentsCommonConfig {

  val path = defaultConfig.getString("home.dir.beers")

  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("missfielded values injector") {
      session => {
        Seq(0.01, 0.05, 0.1).foreach(err => {
          val dirtyDF: DataFrame = run(session, err)
          // dirtyDF.where(col("state") =!= "").show()
          WriterUtil.persistCSV(dirtyDF, s"$path/beers_missfielded_$err")
        })
      }
    }

  }

  def run(session: SparkSession, err: Double): DataFrame = {
    val beersAndBreweriesPath = allCleanData.getOrElse("beers", "")
    val beersSchema = BeersSchema.getSchema
    val cleanBeersBreweriesDF: DataFrame = DataSetCreator
      .createFrame(session, beersAndBreweriesPath, beersSchema: _*)

    val cleanPercentage: Double = 1.0 - err
    val Array(city_to_dirty, remains_clean_1) = cleanBeersBreweriesDF
      .randomSplit(Array(err, cleanPercentage), 123L)
    val tmpCol = "tmp-city"
    val dirtyCityDF: DataFrame = city_to_dirty.withColumn(tmpCol, concat_ws(" ", col(city), col(state)))
      .drop(state)
      .drop(city)
      .withColumn(state, lit(""))
      .withColumnRenamed(tmpCol, city)
    val dirty1DF: DataFrame = dirtyCityDF
      .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)
      .union(remains_clean_1)
      .toDF(beersSchema: _*)
    dirty1DF
  }
}

object BeersWrongDataTypeInjector extends ExperimentsCommonConfig {
  val path = defaultConfig.getString("home.dir.beers")

  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("wrong data type injector") {
      session => {
        Seq(0.01, 0.05, 0.1).foreach(err => {
          val dirtyDF: DataFrame = run(session, err)
          dirtyDF.show()
          //WriterUtil.persistCSV(dirtyDF, s"$path/beers_wrongdatatype_$err")
        })
      }
    }

  }


  def run(session: SparkSession, err: Double): DataFrame = {
    val beersAndBreweriesPath = allCleanData.getOrElse("beers", "")
    val beersSchema = BeersSchema.getSchema
    val cleanBeersBreweriesDF: DataFrame = DataSetCreator
      .createFrame(session, beersAndBreweriesPath, beersSchema: _*)

    val cleanPercentage: Double = 1.0 - err
    val splitArray = Array(err, cleanPercentage)
    val Array(abv_to_dirty, remains_clean_1) = cleanBeersBreweriesDF.randomSplit(splitArray, 123L)

    //adding % to the integer value
    val dirtyAbvDF: DataFrame = abv_to_dirty
      .withColumn("tmp-abv", concat(col("abv"), lit("%")))
      .drop("abv")
      .withColumnRenamed("tmp-abv", "abv")


    val dirty1DF: DataFrame = dirtyAbvDF
      .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)
      .union(remains_clean_1)
      .toDF(beersSchema: _*)

    val Array(ouncesToDirtyDF, remains_clean_2) = dirty1DF.randomSplit(splitArray, 345L)

    // adding oz. string to the int value
    val ounce = "oz."
    val dirtyOuncesDF: DataFrame = ouncesToDirtyDF.withColumn("ounce-tmp", concat_ws(" ", col("ounces"), lit(ounce)))
      .drop("ounces")
      .withColumnRenamed("ounce-tmp", "ounces")
      .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)


    val dirty2DF: DataFrame = dirtyOuncesDF
      .select("tid", "id", "beer-name", "style", "ounces", "abv", "ibu", "brewery_id", "brewery-name", city, state)
      .union(remains_clean_2)
      .toDF(beersSchema: _*)

    dirty2DF
  }
}
