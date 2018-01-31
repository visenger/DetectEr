package de.model.util


import scala.math.BigDecimal.RoundingMode

/**
  * Created by visenger on 16/02/17.
  */
object NumbersUtil {


  def round(percentageFound: Double, scale: Int = 2): Double = {
    val number: Double = percentageFound.isNaN match {
      case true => 0.0
      case false => percentageFound
    }
    BigDecimal(number).setScale(scale, RoundingMode.HALF_UP).toDouble
  }

  def percentageFound(total: Double, found: Double, msg: String): Double = {
    val percent: Double = round(((found * 100) / total), 4)
    percent
  }


}
