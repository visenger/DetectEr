package de.evaluation.f1.trifacta

import com.typesafe.config.ConfigFactory
import de.evaluation.data.blackoak.BlackOakGoldStandard
import de.evaluation.data.schema.BlackOakSchema
import de.evaluation.f1.Eval
import de.evaluation.util.{DataSetCreator, SparkSessionCreator}
import org.apache.spark.sql._


class TrifactaEvaluator {

  private val conf = ConfigFactory.load()
  private val cleanData = "data.BlackOak.clean-data-path"
  private val dirtyData = "data.BlackOak.dirty-data-path"
  private val trifactaData = "trifacta.cleaning.result"


  def writeResults(): Eval = {

    //see trifacta script to decide which attributes should participate in evaluation

    val sparkSession: SparkSession = SparkSessionCreator.createSession("TFCT")

    val recid = "RecID"
    val zip = "ZIP"
    val ssn = "SSN"
    val attributesNames = List(recid, zip, ssn)

    /*what was cleaned*/
    val trifactaDF: DataFrame = DataSetCreator
      .createDataSet(sparkSession, trifactaData, BlackOakSchema.schema: _*)
    // trifactaDF.show(5)
    val trifactaCols: List[Column] = getColumns(trifactaDF, attributesNames)
    val trifactaProjection: DataFrame = trifactaDF.select(trifactaCols: _*)

    /*what is dirty*/
    val dirtyBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, dirtyData, BlackOakSchema.schema: _*)
    //  dirtyBlackOakDF.show(5)
    val dirtyDFCols: List[Column] = getColumns(dirtyBlackOakDF, attributesNames)
    val dirtyDataProjection = dirtyBlackOakDF.select(dirtyDFCols: _*)

    /*what is clean*/
    val cleanBlackOakDF: DataFrame = DataSetCreator.createDataSet(sparkSession, cleanData, BlackOakSchema.schema: _*)

    val cleanDataCols: List[Column] = getColumns(cleanBlackOakDF, attributesNames)
    val cleanDataProjection: DataFrame = cleanBlackOakDF.select(cleanDataCols: _*)


    val whatTrifactaFound = dirtyDataProjection.except(trifactaProjection)
    //whatTrifactaFound.show(5)

    val dirtyTotal: Dataset[Row] = dirtyDataProjection.except(cleanDataProjection)
    // dirtyTotal.show(4)

    val truePositivesDS: Dataset[Row] = dirtyTotal.intersect(whatTrifactaFound)

    val tp = truePositivesDS.count()

    val fp: Long = whatTrifactaFound.count() - tp

    val fn: Long = dirtyTotal.count() - tp

    val precision: Double = tp.toDouble / (tp + fp)
    val recall: Double = tp.toDouble / (tp + fn)
    val f1: Double = (2 * precision * recall) / (precision + recall)

    val evalTrifacta = Eval(precision, recall, f1)


    sparkSession.stop()
    evalTrifacta
  }

  private def getColumns(df: DataFrame, attributesNames: List[String]) = {
    attributesNames.map(attr => df.col(attr))
  }
}

object TrifactaEvaluator {

  def main(args: Array[String]): Unit = {
    val eval = new TrifactaEvaluator().writeResults()
    eval.printResult("trifacta")
  }

}
