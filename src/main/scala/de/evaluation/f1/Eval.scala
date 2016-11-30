package de.evaluation.f1

/**
  * Created by visenger on 16/11/16.
  */


case class Eval(precision: Double, recall: Double, f1: Double) {
  override def toString: String = {
    s"precision: $precision, recall: $recall, F1: $f1"
  }

  def printResult(info: String): Unit = {

    println(s"$info : ${toString}")
  }
}
