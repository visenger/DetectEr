package de.error.detection.from.metadata

import de.util.ErrorNotation.{CLEAN, DOES_NOT_APPLY, ERROR}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object UDF {

  // Error Classifier # values with low probabilities are suspicious
  val is_value_with_low_counts = udf {
    (numOfTuples: Long, columnDistinctVals: Int, value: String, valuesWithCounts: Map[String, Int]) => {

      var result = DOES_NOT_APPLY
      //todo: ValuesWithCounts are not optimal for flights

      if (numOfTuples == columnDistinctVals) result = DOES_NOT_APPLY
      else {
        if (valuesWithCounts.contains(value)) {
          val counts: Int = valuesWithCounts.getOrElse(value, 0)
          result = if (counts > 1) CLEAN else ERROR
        } else result = DOES_NOT_APPLY
      }
      result
    }
  }

  //Error Classifier: 6 # misfielded values
  //analysing the pattern length distribution and selecting the trimmed distribution
  //if the value is inside then clean otherwise error
  //if number of distinct pattern length <=3 then does-not-apply
  /**
    *
    * param value        :String the cell value
    * param valuesLength : Seq[Int] the set of the trimmed distribution (threshold 10%) of values pattern length
    */
  val is_value_pattern_length_within_trimmed_distr = udf {
    (value: String, valuesLength: Seq[Int]) => {
      var result = DOES_NOT_APPLY

      if (value == null) result = DOES_NOT_APPLY
      else {
        result = if (valuesLength.contains(value.size)) DOES_NOT_APPLY //here: we cannot say anything about the CLEAN
        else ERROR
      }
      result
    }
  }

  //Error classifier #3: top-values (aka histogram)
  val is_in_top_10_value = udf {
    (value: String, attrName: String, top10Values: mutable.Seq[String]) => {

      var result: Int = DOES_NOT_APPLY

      if (value != null) {

        val valueInTop10: Boolean = top10Values.map(_.toLowerCase()).contains(value.toLowerCase())
        valueInTop10 match {
          case true => result = CLEAN
          case false => result = ERROR
        }
      }
      result
    }
  }

  val identify_missing = udf {
    isNull: Boolean =>
      isNull match {
        case true => ERROR
        case false => CLEAN
      }
  }

  def majority_vote = udf {
    classifiers: mutable.WrappedArray[Int] => {

      //      val identifierToAmount: Map[Int, Int] = classifiers.groupBy(v => v).map(t => {
      //        val value: Int = t._1
      //        val amount: Int = t._2.size
      //        (value, amount)
      //      })
      //
      //      val majorityTuple: (Int, Int) = identifierToAmount.maxBy(t => t._2)
      //      val majorityElement: Int = majorityTuple._1
      //
      //      val result = majorityElement match {
      //        case ERROR => ERROR
      //        case _ => CLEAN
      //      }

      val totalSum: Int = classifiers.sum
      val result: Int = if (totalSum > 0) ERROR else CLEAN
      result
    }
  }

  def min_1 = udf {
    classifiers: mutable.WrappedArray[Int] => {
      val result: Int = if (classifiers.contains(ERROR)) ERROR else CLEAN
      result
    }
  }

}
