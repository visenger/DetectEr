package de.model.util

object Features {

  val toolsVector: String = "tools-vector"
  val contentBasedVector: String = "metadata"
  val dependenciesVector: String = "fds"
  val generalVector: String = "general"
  val fullMetadata: String = "full-metadata"

  val featuresCol: String = "features"

  val allFeatures = Seq(toolsVector, contentBasedVector, dependenciesVector, generalVector)

}
