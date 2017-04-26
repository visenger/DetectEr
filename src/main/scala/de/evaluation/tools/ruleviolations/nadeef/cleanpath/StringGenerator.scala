package de.evaluation.tools.ruleviolations.nadeef.cleanpath

object StringGenerator extends App {
  val rules: String = (0 to 45).map(d => s"detect $d").mkString("\n")

  val datasetName = "flights"

  println(
    s"""load ./$datasetName/cleanpath.json
       |$rules
     """.stripMargin)


}

