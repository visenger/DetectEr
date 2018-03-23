package de.model.util


import scala.math.BigDecimal.RoundingMode

/**
  * Created by visenger on 16/02/17.
  */
object NumbersUtil {


  def round(inputNumber: Double, scale: Int = 2): Double = {
    //println(s"number to round: $inputNumber is NaN ${inputNumber.isNaN} is Infinity ${inputNumber.isInfinity}")

    var number: Double = 0.0

    number = inputNumber.isNaN match {
      case true => 0.0
      case false => inputNumber.isInfinity match {
        case true => 0.0
        case false => inputNumber
      }
    }



    BigDecimal(number).setScale(scale, RoundingMode.HALF_UP).toDouble
  }

  def percentageFound(total: Double, found: Double, msg: String): Double = {
    val percent: Double = round(((found * 100) / total), 4)
    percent
  }


}
