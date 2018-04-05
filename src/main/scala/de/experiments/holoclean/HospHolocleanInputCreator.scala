package de.experiments.holoclean

import de.evaluation.util.{DataSetCreator, SparkLOAN}
import de.experiments.holoclean.HospHolocleanSchema._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class HospHolocleanInputCreator {

}

case class ColumnsPair(col1: String, col2: String)

object HospHolocleanPredicatesCreator {

  val dirtyDataPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/hospital_dataset.csv"
  val pathToData = "/Users/visenger/deepdive_notebooks/holoclean-hosp/"

  /**
    * we simulate the perfect error detection, so the holoclean(deepdive) is getting its chance to shine.
    */
  val groundtruthPath = "/Users/visenger/deepdive_notebooks/holoclean-hosp/datasets/groundtruth.csv"
  val attrCol = "attr"
  val valueCol = "value"

  def main(args: Array[String]): Unit = {

    SparkLOAN.withSparkSession("holoclean hosp predicates") {
      sesssion => {

        val dirtyDataDF: DataFrame = DataSetCreator.createFrame(sesssion, dirtyDataPath, HospHolocleanSchema.schema: _*)
        //dirtyDataDF.printSchema()


        /**
          * tuple (
          * tid bigint
          * ).
          **/

        val tupleDF: DataFrame = dirtyDataDF.select(col(HospHolocleanSchema.index))
        /* tupleDF
           .repartition(1)
           .write
           .option("header", "false")
           .csv(s"$pathToData/input/tuple")*/

        /**
          * initvalue (
          * tid bigint,
          * attr int,
          * value text
          * ).
          **/

        val convert_to_name = udf {
          idx: Int => {
            HospHolocleanSchema.idxToAttr.getOrElse(idx, "unknown")
          }
        }

        val initvalueDF: DataFrame = dirtyDataDF.withColumn("valsToArray",
          array(index,
            ProviderNumber,
            HospitalName,
            Address1,
            Address2,
            Address3,
            City,
            State,
            ZipCode,
            CountyName,
            PhoneNumber,
            HospitalType,
            HospitalOwner,
            EmergencyService,
            Condition,
            MeasureCode,
            MeasureName,
            Score,
            Sample,
            Stateavg))
          .select(col(index), posexplode(col("valsToArray")).as(Seq(attrCol, valueCol)))
          .where(col(attrCol) =!= 0)
          .toDF(index, attrCol, valueCol)


        initvalueDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/initvalue")


        /**
          * domain (
          * tid bigint,
          * attr int,
          * domain text
          * ).
          **/

        //todo done: create ideal error detection
        val groundTruthPrepareDF: DataFrame = DataSetCreator.createFrame(sesssion, groundtruthPath, Seq("ind", attrCol, "val"): _*)

        val convert_to_idx = udf {
          attrName: String => {
            HospHolocleanSchema.attrToIdx.getOrElse(attrName, 0)
          }
        }

        val groundTruthDF: DataFrame = groundTruthPrepareDF
          .withColumn(s"$attrCol-idx", convert_to_idx(groundTruthPrepareDF(attrCol)))
          .drop(col(attrCol))
          .withColumnRenamed(s"$attrCol-idx", attrCol)
          .select("ind", attrCol, "val")
          .toDF(index, attrCol, valueCol)

        val cleanValuesDF: DataFrame = initvalueDF
          .intersect(groundTruthDF)
          .toDF(index, attrCol, valueCol)

        val dirtyValuesDF: DataFrame = initvalueDF
          .except(groundTruthDF)
          .toDF(index, attrCol, valueCol)

        // get clean values and use these clean values to populate domain
        val domainValues: DataFrame = cleanValuesDF
          .groupBy(col(attrCol))
          .agg(collect_set(col(valueCol)).as("domain"))

        val domainDF: DataFrame = initvalueDF
          .join(domainValues, Seq(attrCol), "full_outer")
          .select(initvalueDF(index), col(attrCol), explode(col("domain")).as(valueCol))

        domainDF
          .repartition(1)
          .write
          .option("sep", "\\t")
          .option("header", "false")
          .csv(s"$pathToData/input/domain")


        /**
          * value? (
          * tid bigint,
          * attr int,
          * value text
          * ).
          */

        val preparedValueDF: DataFrame =
          pruneDomainHospHoloclean(sesssion, initvalueDF, domainValues, dirtyValuesDF, dirtyDataDF)


        (0.0 to 1.0).by(0.1).foreach(τ => {

          val prunedValueDF: DataFrame = preparedValueDF
            .where(preparedValueDF("prob-of-domain") >= τ)
            .select(index, attrCol, valueCol)
            .withColumn("label", lit("\\N"))
            .where(col(index) =!= lit(""))
            .select(index, attrCol, "value", "label")

          /*prunedValueDF
            .repartition(1)
            .write.option("sep", "\\t")
            .option("header", "false")
            .csv(s"$pathToData/input-${τ}/value-${τ}")*/
        })

        /* valueDF.repartition(1)
           .write
           .option("sep", "\\t")
           .option("header", "false")
           .csv(s"$pathToData/input/value")*/

      }
    }


  }

  def pruneDomainHospHoloclean(session: SparkSession,
                               initValueDF: DataFrame,
                               cleanDomainByAttrDF: DataFrame,
                               dirtyValuesDF: DataFrame,
                               dirtyDataDF: DataFrame): DataFrame = {

    import session.implicits._

    var attrCombiToCooccurrDFs: Map[ColumnsPair, DataFrame] = Map()

    //pre-process the cooccurrences
    HospHolocleanSchema.schema
      .filterNot(_.equals(index))
      .combinations(2)
      .foreach(pair => {
        val col1: String = pair(0)
        val col2: String = pair(1)

        //          val col1 = City
        //          val col2 = ZipCode
        val countCol = s"count-$col1-$col2"
        val prob1Col = s"prob-$col1-given-$col2"
        val prob2Col = s"prob-$col2-given-$col1"
        val countFirstCol = s"count-$col1"
        val countSecondCol = s"count-$col2"

        val cooccurrDF: DataFrame = dirtyDataDF
          .groupBy(dirtyDataDF(col1), dirtyDataDF(col2))
          .count()
          .toDF(col1, col2, countCol)

        //            cooccurrDF.printSchema()
        //println(s"count coocurences between [$col1] and [$col2]")

        val windowCol1: WindowSpec = Window.partitionBy(col(col1))

        val windowCol2: WindowSpec = Window.partitionBy(col(col2))

        val cooccurrWithProbsDF: DataFrame = cooccurrDF
          .withColumn(countFirstCol, sum(countCol) over windowCol1)
          .withColumn(countSecondCol, sum(countCol) over windowCol2)
          .withColumn(prob1Col, lit(col(countCol) / col(countSecondCol)))
          .withColumn(prob2Col, lit(col(countCol) / col(countFirstCol)))

        attrCombiToCooccurrDFs += (ColumnsPair(col1, col2) -> cooccurrWithProbsDF)
        attrCombiToCooccurrDFs += (ColumnsPair(col2, col1) -> cooccurrWithProbsDF)
      })


    //input columns: [index, attr, value]
    val otherTupleValues = "other-vals"
    /**
      * first aggregate by RecID, second for each tuple in aggregated bin,
      * concatenate its values as Map "attrNr->value"
      */
    val otherValsOfTuple: DataFrame = initValueDF.groupBy(col(index))
      .agg(collect_list(map(col(attrCol), col(valueCol))) as otherTupleValues) //collects Map[String, String] as list
      .as[(String, Seq[Map[String, String]])]
      .map { case (id, list) => (id, list.reduce(_ ++ _)) } //joining all maps in the list
      .toDF(index, otherTupleValues)

    /**
      *
      * otherValsOfTuple.printSchema()
      * root
      * |-- index: string (nullable = true)
      * |-- other-vals: map (nullable = true)
      * |    |-- key: string
      * |    |-- value: string (valueContainsNull = true)
      *
      */

    val domainWithTupleValuesDF: DataFrame = dirtyValuesDF
      .join(cleanDomainByAttrDF, Seq(attrCol), "full_outer")
      .join(otherValsOfTuple, Seq(index))


    /**
      *domainWithTupleValuesDF.printSchema()
      * root
      * 0|-- index: string (nullable = true)
      * 1|-- attr: integer (nullable = true)
      * 2|-- value: string (nullable = true)
      * 3|-- domain: array (nullable = true)
      * |    |-- element: string (containsNull = true)
      * 4|-- other-vals: map (nullable = true)
      * |    |-- key: string
      * |    |-- value: string (valueContainsNull = true)
      * *
      *
      * +-----+----+--------------------+--------------------+--------------------+
      * |index|attr|               value|              domain|          other-vals|
      * +-----+----+--------------------+--------------------+--------------------+
      * |  172|   2|MARSHALL MEDICAL ...|[MARION REGIONAL ...|Map(12 -> Governm...|
      * |  200|  14|        Heaxt Attack|[Heart Attack, Su...|Map(12 -> Volunta...|
      */


    val errorCellsWithDomain: DataFrame = domainWithTupleValuesDF
      .rdd
      .collect()
      //.take(3)
      .map(row => {
      println(s"processing row: ${row.mkString("; ")}")
      val index: String = row.getString(0)
      val attrNr: Int = row.getInt(1)
      val value: String = row.getString(2)
      val domain: List[String] = row.getSeq[String](3).toList
      val otherValsOfTuple: collection.Map[String, String] = row.getMap[String, String](4).toMap

      val otherValsOfTupleWithoutCurrentAttr = otherValsOfTuple
        .filter(tuple => !tuple._1.equalsIgnoreCase(attrNr.toString)).toMap

      val tuples: Seq[(String, Double)] = for {domainValue <- domain;
                                               other <- otherValsOfTupleWithoutCurrentAttr} yield {

        val otherColIdx: Int = other._1.toInt
        val otherValue: String = other._2.toString

        val domainAttrName: String = HospHolocleanSchema.idxToAttr.getOrElse(attrNr, "unknown")
        val otherAttrName: String = HospHolocleanSchema.idxToAttr.getOrElse(otherColIdx, "unknown")

        //          println(s"getting $domainAttrName and $otherAttrName")
        //          println(s"cooccurr dict size: ${attrCombiToCooccurrDFs.size}")
        //
        //          println(s"dict contains key: ${attrCombiToCooccurrDFs.contains(ColumnsPair(domainAttrName, otherAttrName))} ")
        //          println(s"cooccurr size: ${
        //            attrCombiToCooccurrDFs.get(
        //              (ColumnsPair(domainAttrName, otherAttrName))).get.count()
        //          } ")


        val cooccurrDF: DataFrame = attrCombiToCooccurrDFs.get(
          (ColumnsPair(domainAttrName, otherAttrName))).get.cache()

        val probCol = s"prob-$domainAttrName-given-$otherAttrName"
        val cooccurrRow: DataFrame = cooccurrDF
          .where(cooccurrDF(domainAttrName) === domainValue && cooccurrDF(otherAttrName) === otherValue)
          .select(probCol)

        val probOfDomainValue: Double = cooccurrRow.take(1).isEmpty match {
          case true => 0.0
          case false => cooccurrRow.head().getDouble(0)
        }

        (domainValue, probOfDomainValue)

      }
      val domainWithProbs: Map[String, Double] = tuples.toMap.filterNot(_._2 == 0.0)

      (index, attrNr, value, domainWithProbs)
    }).toList.toDF(index, attrCol, valueCol, "domain-with-probs")

    val errorsWithDomainDF: DataFrame = errorCellsWithDomain
      .select(col(index), col(attrCol), explode(col("domain-with-probs")).as(Seq("value", "prob-of-domain")))

    errorsWithDomainDF

  }

}
