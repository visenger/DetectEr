package de.model.util


import scala.math.BigDecimal.RoundingMode

/**
  * Created by visenger on 16/02/17.
  */
object NumbersUtil {


  def round(percentageFound: Double, scale: Int = 2) = {
    val number = percentageFound.isNaN match {
      case true => 0.0
      case false => percentageFound
    }
    BigDecimal(number).setScale(scale, RoundingMode.HALF_UP).toDouble
  }

}
