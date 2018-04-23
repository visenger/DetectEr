package de.deepdive.error.detection

import de.evaluation.util.DataSetCreator
import de.holoclean.ZipCodesDictSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object ExternalDictionariesPredicatesCreator {

  def createZipCityStatesPredicates(session: SparkSession, pathToExtDict: String): (DataFrame, DataFrame, DataFrame) = {
    val zipCodesDF: DataFrame = DataSetCreator
      .createFrame(session, pathToExtDict, ZipCodesDictSchema.schema: _*)
    val zipDF: DataFrame = zipCodesDF
      .select(col(ZipCodesDictSchema.zipCode))
      .distinct()
      .toDF(ZipCodesDictSchema.zipCode)

    val cityDF: DataFrame = zipCodesDF
      .select(ZipCodesDictSchema.city)
      .distinct()
      .toDF(ZipCodesDictSchema.city)

    val stateDF: DataFrame = zipCodesDF
      .select(ZipCodesDictSchema.state)
      .distinct()
      .toDF(ZipCodesDictSchema.state)

    (zipDF, cityDF, stateDF)
  }



}

