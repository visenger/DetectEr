package de.evaluation.util

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * LOAN Pattern for Spark session handling: close session after performing some function f.
  */
object SparkLOAN {

  def withSparkSession(name: String)(f: SparkSession => Unit): Unit = {
    val r = SparkSessionCreator.createSession(name)
    try {
      f(r)
    } finally {
      r.stop()
    }

  }

  /* def withSparkSession(name: String)(f: SparkSession => DataFrame) = {
     val r = SparkSessionCreator.createSession(name)
     try {
       f(r)
     } finally {
       r.stop()
     }
   }*/

}
