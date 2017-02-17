package de.model.util


import scala.math.BigDecimal.RoundingMode

/**
  * Created by visenger on 16/02/17.
  */
object NumbersUtil {


  def round(percentageFound: Double, scale: Int = 2) = {
    BigDecimal(percentageFound).setScale(scale, RoundingMode.HALF_UP).toDouble
  }

}
