package de.model.util

import de.evaluation.f1.Table

/**
  * Created by visenger on 28/12/16.
  */
object Model {

  val recId = Table.recid
  val attrNr = Table.attrnr
  val indexedcol = "RecIdIDX"
  val tools: Seq[String] = (1 to 5).map(i => s"${Table.exists}-$i")
  private val ids = Seq(Table.recid, Table.attrnr)
  val schema: Seq[String] = ids ++ tools

  val extendedSchema: Seq[String] = schema ++ Seq(indexedcol)

}
