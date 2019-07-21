package com.spark

import com.typesafe.config.ConfigFactory

object Config {

  private lazy val defaultConfig = ConfigFactory.load("application.conf")

  private val config = ConfigFactory.load().withFallback(defaultConfig)

  private lazy val root = config.getConfig("configuration")

  lazy val appName = root.getString("appName")

  lazy val master = root.getString("master")

  lazy val path = root.getString("path")

}
