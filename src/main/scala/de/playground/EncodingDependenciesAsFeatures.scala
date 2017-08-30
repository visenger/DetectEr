package de.playground

import com.typesafe.config.{Config, ConfigFactory}
import de.evaluation.data.schema.HospSchema
import de.evaluation.f1.FullResult
import de.evaluation.util.{DataSetCreator, SparkLOAN}
import org.apache.spark.sql.DataFrame

class EncodingDependenciesAsFeatures {

}

object EncodingDependenciesAsFeaturesPlayground {
  def main(args: Array[String]): Unit = {

    val config: Config = ConfigFactory.load()

    val dirtyData = "data.hosp.dirty.10k"


    /*
    * *
    FD1: zip -> city
    FD2: zip -> state
    *
    *
    *
    *  @param joinType Type of join to perform. Default `inner`. Must be one of:
   *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   *                 `right`, `right_outer`, `left_semi`, `left_anti`.
   *
    * */


    SparkLOAN.withSparkSession("FD-ENCODING") {
      session => {
        import org.apache.spark.sql.functions._

        val dirtyHospDF = DataSetCreator.createFrame(session, config.getString(dirtyData), HospSchema.getSchema: _*)

        //FD1: zip -> city

        val idxOfLHS: Int = HospSchema.getIndexesByAttrNames(List("zip")).head

        val zipCounts: DataFrame = dirtyHospDF
          .groupBy(dirtyHospDF("zip"))
          .count()

        val clustersForFD: DataFrame = zipCounts
          .where(zipCounts("count") > 1)
          .withColumn("cluster-id", concat_ws("-", lit("clust"), monotonically_increasing_id() + 1))
          .toDF()

        clustersForFD.printSchema()


        println(s"number of clusters for the FD1: ${clustersForFD.count()}")

        val joinedWithGroups: DataFrame = dirtyHospDF
          .join(clustersForFD, Seq("zip"), "left_outer")
          .na.fill(0, Seq("count"))
          .na.fill("clust-zero", Seq("cluster-id"))
        //        joinedWithGroups.show()

        val attributes = HospSchema.getSchema.filterNot(_.equals(HospSchema.getRecID))

        println(s" all attributes: ${attributes.mkString(", ")}")

        //        joinedWithGroups.where("count = 0").show()


        val attrDFs: Seq[DataFrame] = attributes.map(attr => {
          val attrIdx = HospSchema.getIndexesByAttrNames(List(attr)).head
          val attrDF = joinedWithGroups.select(HospSchema.getRecID, attr, "cluster-id")
            .withColumn(FullResult.attrnr, lit(attrIdx))
            .toDF(FullResult.recid, "value", "cluster-id", FullResult.attrnr)
          attrDF
            .select(FullResult.recid, FullResult.attrnr, "value", "cluster-id")
            .toDF()
        })

        val fdsEncoded: DataFrame = attrDFs
          .reduce((df1, df2) => df1.union(df2))
          .repartition(1)
          .toDF(FullResult.recid, FullResult.attrnr, "value", "fd-1")
        fdsEncoded.show(13, false)


      }
    }


  }
}


