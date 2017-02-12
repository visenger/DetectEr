package de.evaluation.data.schema

/**
  * Created by visenger on 09/02/17.
  */
trait Schema {

  // def getIndexesByAttrNames(attributes: List[String]): List[Int]

  def getIndexesByAttrNames(attributes: List[String]): List[Int] = {
    val allAttrToLowerCase = attributes.map(_.toLowerCase)
    val indexes = allAttrToLowerCase.map(attr => {
      indexLCAttributes.getOrElse(attr, 0)
    })
    indexes.sortWith(_ < _)
  }

   def indexLCAttributes: Map[String, Int] = {
    indexAttributes.map(a => (a._1.toLowerCase, a._2))
  }

  def getSchema: Seq[String]

  def getRecID: String

  def indexAttributes = {
    getSchema.zipWithIndex.toMap.map(
      e => (e._1, {
        var i = e._2;
        i += 1; // 1-based indexing
        i
      }))
  }

}

