package de.wrangling

import java.io.File

import de.evaluation.util.SparkLOAN
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * preprocess data with: LC_CTYPE=C sed -i.bak 's/,/_/g' *-data.txt
  */
object WrangleFlights {

  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("WRANGLER") {
      session => {

        val dir = "/Users/visenger/research/datasets/clean_flight/"
        val cleanDir = "/Users/visenger/research/datasets/clean_flight/flight_truth/"

        val trueFlightsSchema = Seq("Flight#", "t_Scheduled departure", "t_Actual departure", "t_Departure gate", "t_Scheduled arrival", "t_Actual arrival", "t_Arrival gate")
        val flightsSchema = Seq("Source", "Flight#", "d_Scheduled departure", "d_Actual departure", "d_Departure gate", "d_Scheduled arrival", "d_Actual arrival", "d_Arrival gate")


        val dirtyFilesDir = new File(dir)
        val cleanFilesDir = new File(cleanDir)

        val allDirtyFiles: Array[File] = dirtyFilesDir.listFiles()

        println(s"dirty files: ${allDirtyFiles.mkString(",")}")

        val allJoinedFlights: List[DataFrame] = allDirtyFiles
          .filter(file => file.isFile && file.getName.endsWith("txt"))
          .map(f => {

            val dirtyFileName = f.getName
            val truthFileName = dirtyFileName.replace("data", "truth")
            //val flightTruthFile = "/Users/visenger/research/datasets/clean_flight/flight_truth/2011-12-04-truth.txt"
            val flightTruthFile = s"$cleanDir$truthFileName"
            //          val flightFile = "/Users/visenger/research/datasets/clean_flight/2011-12-04-data.txt"
            val flightFile = s"$dir$dirtyFileName"

            val flights: DataFrame = session
              .read
              .option("delimiter", "\\t")
              .csv(flightFile)
              .toDF(flightsSchema: _*)

            val trueFlights: DataFrame = session
              .read
              .option("delimiter", "\\t")
              .csv(flightTruthFile)
              .toDF(trueFlightsSchema: _*)

            //flights.withColumn("x4New", regexp_replace(flights("x4"), "\\,", ".")).show



            val joinedFlights = flights
              .join(trueFlights, "Flight#")

            joinedFlights
          }).toList

        val flights: DataFrame = allJoinedFlights.reduce((df1, df2) => df1.union(df2))
        val rowid = "RowId"
        val joinedFlights = flights.withColumn(rowid, monotonically_increasing_id())
        joinedFlights.printSchema()

        val schema = Seq(rowid, "Source", "Flight", "ScheduledDeparture", "ActualDeparture", "DepartureGate", "ScheduledArrival", "ActualArrival", "ArrivalGate")

        val cleanFlights = joinedFlights
          .select(rowid, Seq("Source") ++ trueFlightsSchema: _*)
          .toDF(schema: _*)

        cleanFlights.show(false)

        val dirtyFlights = joinedFlights
          .select(rowid, flightsSchema: _*)
          .toDF(schema: _*)

        dirtyFlights.show(false)

        val dirtyDataPath = s"${dir}dirty-data"
        val cleanDataPath = s"${cleanDir}ground-truth"

        cleanFlights
          .coalesce(1)
          .write
          .option("header", true)
          .csv(cleanDataPath)

        dirtyFlights
          .coalesce(1)
          .write
          .option("header", true)
          .csv(dirtyDataPath)


      }
    }

  }

}
