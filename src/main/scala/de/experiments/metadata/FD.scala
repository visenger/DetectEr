package de.experiments.metadata

case class FD(lhs: List[String], rhs: List[String]) {
  def getFD: List[String] = lhs ::: rhs

  def getLHS: List[String] = lhs

  def getRHS: List[String] = rhs

  override def toString = {
    s"${lhs.mkString(", ")} -> ${rhs.mkString(", ")}"
  }
}
