package de.error.detection.from.metadata

import de.evaluation.f1.{Eval, F1}
import de.evaluation.util.SparkLOAN
import de.experiments.ExperimentsCommonConfig
import de.experiments.features.generation.FeaturesGenerator
import de.experiments.metadata.FD
import de.model.util.FormatUtil
import de.util.DatasetFlattener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
  * TODO: to be merged with MetadataBasedErrorDetector
  **/
object SchemaMetadataErrDetector extends ExperimentsCommonConfig with ConfigBase {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("FD based error detection") {
      session => {
        val datasets = Seq("beers", "blackoak", "flights", "museum")
        //val datasets = Seq("hosp")

        datasets.foreach(dataset => {
          println(s"processing $dataset")

          val generator: FeaturesGenerator = FeaturesGenerator().onDatasetName(dataset)
          val allFDs: List[FD] = generator.allFDs
          println(allFDs.mkString("; "))

          val recID: String = generator.getSchema.getRecID

          val dirtyFrame: DataFrame = generator.getDirtyData(session)

          /**
            * +----+------------+---+--------------------+
            * | tid|    attrName|pos|               value|
            * +----+------------+---+--------------------+
            **/

          val datasetFlattener: DatasetFlattener = DatasetFlattener().onDataset(dataset)
          val dirtyDataFlattenedDF: DataFrame = datasetFlattener.flattenDataFrame(dirtyFrame)
          dirtyDataFlattenedDF.show(5)

          val flatDirtyColumns: Array[Column] = dirtyDataFlattenedDF.schema.fields.map(f => col(f.name))


          //todo: compute FD compliance
          val fdViolations: List[DataFrame] = allFDs.map(fd => {
            println(s"FD: ${fd.toString}")
            val lhsAttributes: List[String] = fd.getLHS
            val rhsAttributes: List[String] = fd.getRHS

            val lhsColumns: List[Column] = lhsAttributes.map(attr => col(attr))

            val aggregators: List[Column] = rhsAttributes.map(attr => collect_set(attr).as(s"aggregated-$attr"))

            val allIds = "all-ids"
            var fdCompilanceDF: DataFrame = dirtyFrame.select(recID, fd.getFD: _*)
              .groupBy(lhsColumns: _*)
              .agg(collect_set(col(recID)) as allIds, aggregators: _*)

            //todo: filter potential errors -> aggregated RHS is either empty or number of elements are >1;
            rhsAttributes.foreach(rhsAttr => {
              fdCompilanceDF = fdCompilanceDF
                .withColumn(s"size-$rhsAttr", size(fdCompilanceDF(s"aggregated-$rhsAttr")))
            })

            val rhsAttribute: String = rhsAttributes.head //we assume normalized functional dependencies

            val fdErrorDF: DataFrame = fdCompilanceDF
              .where(col(s"size-$rhsAttribute") =!= 1)
              .withColumn("id", explode(col(allIds)))
              .drop(allIds).toDF()


            val fdViolationDF: DataFrame = dirtyDataFlattenedDF
              .join(fdErrorDF,
                dirtyDataFlattenedDF(recID) === fdErrorDF("id") and dirtyDataFlattenedDF("attrName") === lit(rhsAttribute),
                "inner")
              .select(flatDirtyColumns: _*).toDF()

            fdViolationDF

          })

          //got all fd violations
          var allFDViolationsDF: DataFrame = fdViolations
            .reduce((df1, df2) => df1.union(df2))

          //got all clean values
          val noFDViolationDF: DataFrame = dirtyDataFlattenedDF
            .except(allFDViolationsDF)
            .withColumn("fd-compilation", lit(0).cast("int"))

          //add 1 as a indicator for the fd violation
          allFDViolationsDF = allFDViolationsDF
            .withColumn("fd-compilation", lit(1).cast("int"))

          //union to get all values into one DF
          val fullFDComplianceDF: DataFrame = allFDViolationsDF.union(noFDViolationDF).toDF()
          fullFDComplianceDF.printSchema()

          val labelsDF: DataFrame = datasetFlattener.makeFlattenedDiff(session)

          val fdComplianceWithLabelsDF: DataFrame = fullFDComplianceDF.join(labelsDF, Seq(recID, "attrName"), "inner")

          val errDetectionByFDsDF: DataFrame = FormatUtil
            .getPredictionAndLabelOnIntegersForSingleClassifier(fdComplianceWithLabelsDF, "fd-compilation")
          val eval: Eval = F1.evalPredictionAndLabels_TMP(errDetectionByFDsDF)
          eval.printResult(s"fd compliance for $dataset: ")


        })
      }
    }
  }

}
