package de.experiments.metadata

case class FD(lhs: List[String], rhs: List[String]) {
  def getFD: List[String] = lhs ::: rhs

  override def toString = {
    s"${lhs.mkString(", ")} -> ${rhs.mkString(", ")}"
  }
}
