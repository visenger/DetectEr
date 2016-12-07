package de.evaluation.util

import java.util.Properties

import com.typesafe.config.ConfigFactory

/**
  * Created by visenger on 07/12/16.
  */
object DatabaseProps {

  def getDefaultProps: Properties = {
    val conf = ConfigFactory.load()

    val props: Properties = new Properties()
    props.put("user", conf.getString("db.postgresql.user"))
    props.put("password", conf.getString("db.postgresql.password"))
    props.put("driver", conf.getString("db.postgresql.driver"))

    props
  }

}
