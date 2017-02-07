package de.playground

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._

/**
  * Created by visenger on 23/12/16.
  */
object ConfigTester extends App {

  private val config = ConfigFactory.load()
  private val stringList = config.getStringList("some.list")
  stringList.foreach(println)
  private val allConf = stringList.mkString(",")
  println(allConf)


}
