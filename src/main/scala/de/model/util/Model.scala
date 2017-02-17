package de.model.util

import de.evaluation.f1.GoldStandard

/**
  * Created by visenger on 28/12/16.
  */
object Model {

  val recId = GoldStandard.recid
  val attrNr = GoldStandard.attrnr
  val indexedcol = "RecIdIDX"

  val tools: Seq[String] = (1 to 5).map(i => s"${GoldStandard.exists}-$i")
  val ids = Seq(GoldStandard.recid, GoldStandard.attrnr)
  val schema: Seq[String] = ids ++ tools

  val extendedSchema: Seq[String] = schema ++ Seq(indexedcol)

  val label = "label"
  val schemaWithLabel: Seq[String] = ids ++ Seq(label) ++ tools

  val toolsWithIndex: Map[Int, String] = tools
    .zipWithIndex
    .map(e => (e._1, {
      val idx = e._2 + 1
      idx
    }))
    .map(_.swap)
    .toMap

}
