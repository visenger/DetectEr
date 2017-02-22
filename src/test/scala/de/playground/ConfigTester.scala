package de.playground

import com.typesafe.config.ConfigFactory

/**
  * Created by visenger on 23/12/16.
  */
object ConfigTester extends App {

  private val config = ConfigFactory.load()
  private val element = config.getString("common.element.field")
  println(element)
}
