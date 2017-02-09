package de.evaluation.f1

/**
  * Created by visenger on 16/11/16.
  */


case class Eval(precision: Double, recall: Double, f1: Double) {
  override def toString: String = {
    s"precision: $precision, recall: $recall, F1: $f1"
  }
//& tool-1 & p         & r      & f   \\
  def printResult(info: String): Unit = {

    //println(s"$info : ${toString}")
    println(s"""& $info & $precision         & $recall      & $f1   \\\\""")
  }
}
