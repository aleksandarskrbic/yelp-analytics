package com.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object Job {

  def main(args: Array[String]): Unit = {

  }

  def initSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(Config.appName)
      .master(Config.master)
      .getOrCreate()

    spark
  }

  def loadData(spark: SparkSession, path: String): DataFrame = ???

}

