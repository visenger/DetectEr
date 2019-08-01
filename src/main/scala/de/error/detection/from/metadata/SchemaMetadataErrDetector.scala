package de.error.detection.from.metadata

import de.evaluation.f1.{Eval, F1}
import de.evaluation.util.SparkLOAN
import de.experiments.features.generation.FeaturesGenerator
import de.experiments.metadata.FD
import de.model.util.{FormatUtil, NumbersUtil}
import de.util.DatasetFlattener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

/**
  * TODO: to be merged with MetadataBasedErrorDetector
  * TODO: refactor
  **/
object SchemaMetadataErrDetector {

  def main(args: Array[String]): Unit = {
    SparkLOAN.withSparkSession("FD based error detection") {
      session => {

        val datasets = Seq("museum", "beers", "flights", "blackoak")
        //val datasets = Seq("beers")

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

          val flatDirtyColumns: Array[Column] = dirtyDataFlattenedDF.schema.fields.map(f => col(f.name))


          //todo: compute FD compliance
          val fdViolations: List[DataFrame] = allFDs.map(fd => {

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


          val labelsDF: DataFrame = datasetFlattener.makeFlattenedDiff(session)

          val fdComplianceWithLabelsDF: DataFrame = fullFDComplianceDF.join(labelsDF, Seq(recID, "attrName"), "inner")

          if (dataset.equalsIgnoreCase("museum")) {
            coverageForMUSEUM(fdComplianceWithLabelsDF, Seq("fd-compilation"))
          }
          if (dataset.equalsIgnoreCase("beers")) {
            coverageForBEERS(fdComplianceWithLabelsDF, Seq("fd-compilation"))
          }

          val errDetectionByFDsDF: DataFrame = FormatUtil
            .getPredictionAndLabelOnIntegersForSingleClassifier(fdComplianceWithLabelsDF, "fd-compilation")
          val eval: Eval = F1.evalPredictionAndLabels_TMP(errDetectionByFDsDF)
          eval.printLatexString(s"dataset $dataset: ")


        })
      }
    }
  }

  private def coverageForBEERS(matrixWithECsFromMetadataDF: DataFrame, cols: Seq[String]): Unit = {
    println("MISFIELDED, WRONG DATA TYPE, MISSING, ILLEGAL VALUES")
    cols.foreach(ec => {

      /*MISFIELDED*/
      //col(city), col(state)
      val city = "city"
      val state = "state"
      val missfieldedDF: DataFrame = matrixWithECsFromMetadataDF
        .where(col("attrName") === city or col("attrName") === state)
        .where(col("label") === 1).toDF()
      val totalMisfielded: Long = missfieldedDF.count()

      val coverageByECMisf: Long = missfieldedDF.where(col(ec) === 1).count()

      val misfielded: Double = NumbersUtil.percentageFound(totalMisfielded.toDouble, coverageByECMisf.toDouble)

      //println(s" heuristic $ec coverage of misfielded values = $misfielded")

      /*WRONG DATA TYPE*/
      val abv = "abv"

      val wrongDataTypeDF: DataFrame = matrixWithECsFromMetadataDF.where(col("attrName") === abv)
        .where(col("label") === 1).toDF()

      val totalWrongDtype: Long = wrongDataTypeDF.count()
      val coverageByECWDT: Long = wrongDataTypeDF.where(col(ec) === 1).count()
      val wrongDataType: Double = NumbersUtil.percentageFound(totalWrongDtype.toDouble, coverageByECWDT.toDouble)

      /*DEFAULT_VALUE*/
      /*MISSING*/
      val ibu = "ibu"

      val missingDefaultDF: DataFrame = matrixWithECsFromMetadataDF.where(col("attrName") === ibu)
        .where(col("label") === 1).toDF()
      val totalMissingDefault: Long = missingDefaultDF.count()

      val coverageByECMD: Long = missingDefaultDF.where(col(ec) === 1).count()
      val missingDefault: Double = NumbersUtil.percentageFound(totalMissingDefault.toDouble, coverageByECMD.toDouble)


      /*ILLEGAL VALUES*/
      val ounces = "ounces"
      val illegalValsDF: DataFrame = matrixWithECsFromMetadataDF.where(col("attrName") === ounces)
        .where(col("label") === 1).toDF()

      val totalIllegalVals: Long = illegalValsDF.count()

      val coverageByECIV: Long = illegalValsDF.where(col(ec) === 1).count()
      val illegal: Double = NumbersUtil.percentageFound(totalIllegalVals.toDouble, coverageByECIV.toDouble)

      println(s"heuristic $ec & $misfielded & $wrongDataType & $missingDefault & $illegal")


    })
  }

  private def coverageForMUSEUM(matrixWithECsFromMetadataDF: DataFrame, cols: Seq[String]): Unit = {
    /**
      * Errors coverage by heuristics for MUSEUM
      **/

    val errorTypesMuseum = Seq(
      "#LV_AMBIGUOUS_DATA#",
      "#LV_MISFIELDED#",
      "#LV_UNUSED#",
      "#LV_MISSING#",
      "#LV_EXTRANEOUS_DATA#",
      "#LV_DEFAULT_VALUE#",
      "#LV_DIFFERENT_WORD_ORDERING#")


    cols.foreach(ec => {

      var jointCoverage: mutable.Seq[Double] = mutable.Seq()

      errorTypesMuseum.foreach(errorType => {

        val errorsCount: Long = matrixWithECsFromMetadataDF
          .where(col("clean-value") === errorType)
          .count()

        val coverageByEC: Long = matrixWithECsFromMetadataDF
          .where(col(ec) === 1 and col("clean-value") === errorType)
          .count()

        jointCoverage = jointCoverage :+ NumbersUtil
          .percentageFound(errorsCount.toDouble, coverageByEC.toDouble)

        println(s"heuristic name: $ec, coverage of the $errorType = $coverageByEC")
      })


      println(s"heuristic $ec & ${jointCoverage.mkString(" & ")}")
    })
  }

}
