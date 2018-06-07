package de.evaluation.f1

/**
  * Created by visenger on 16/11/16.
  */


case class Eval(precision: Double, recall: Double, f1: Double, info: String = "") {
  override def toString: String = {
    s"""precision: $precision, recall: $recall, F1: $f1,  $info"""
  }


  def printResult(msg: String): Unit = {
    println(s"$msg : ${toString}")
  }

  def printLatexString(info: String): Unit = {
    import de.model.util.NumbersUtil._
    //& tool-1 & p         & r      & f   \\
    println(s"""& $info & ${round(precision, 4)} & ${round(recall, 4)} & ${round(f1, 4)} \\\\""")
  }
}
