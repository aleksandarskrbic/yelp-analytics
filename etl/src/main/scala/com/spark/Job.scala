package com.spark

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Job {

  def main(args: Array[String]): Unit = {
    val spark = initSparkSession()

    val businessDF = spark.read
      .format("json")
      .load(Config.businessPath)

    val checkinDF = spark.read
      .format("json")
      .load(Config.checkinPath)

    val reviewDF = spark.read
      .format("json")
      .load(Config.reviewPath)

    val userDF = spark.read
      .format("json")
      .load(Config.userPath)

    businessByReview(businessDF)
    businessByCity(businessDF)
    businessByIsOpen(businessDF)
    businessCheckins(spark, businessDF, checkinDF)
    wordCounts(spark, reviewDF, true)
    wordCounts(spark, reviewDF, false)
    userDetails(spark, userDF)

  }

  def initSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(Config.appName)
      .master(Config.master)
      .getOrCreate()

    spark
  }

  def businessByReview(businessDF: DataFrame): Unit = {
    businessDF.select("name", "city", "stars", "review_count")
      .sort(desc("stars"), desc("review_count"))
      .coalesce(1)
      .write
      .option("header", "true")
      .csv("data/businessByReviews")
  }

  def businessByCity(businessDF: DataFrame): Unit = {
    businessDF.select("name", "city", "stars", "review_count")
      .groupBy("city")
      .count()
      .sort(desc("count"))
      .coalesce(1)
      .write
      .option("header", "true")
      .csv("data/businessByCity")
  }

  def businessByIsOpen(businessDF: DataFrame): Unit = {
    businessDF.select("name", "city", "is_open")
      .groupBy("is_open")
      .count()
      .coalesce(1)
      .write
      .option("header", "true")
      .csv("data/businessByIsOpen")
  }

  def businessCheckins(sparkSession: SparkSession, businessDF: DataFrame, checkinDF: DataFrame): Unit = {
    val rdd = businessDF.join(checkinDF,
      businessDF.col("business_id") === checkinDF.col("business_id"),
      "left_outer")
      .select("name", "city", "is_open", "review_count", "stars", "date")
      .rdd
      .filter(!_.anyNull)
      .map {
        row => (row(0).asInstanceOf[String],
                row(1).asInstanceOf[String],
                row(2).asInstanceOf[Long],
                row(3).asInstanceOf[Long],
                row(4).asInstanceOf[Double],
                row(5).asInstanceOf[String].split(",").length)
      }

    sparkSession.createDataFrame(rdd)
      .toDF("name", "city", "is_open", "review_count", "stars", "checkin_count")
      .sort(desc("checkin_count"))
      .coalesce(1)
      .write
      .option("header", "true")
      .csv("data/businessCheckins")
  }

  def wordCounts(sparkSession: SparkSession, reviewDF: DataFrame, topReviews: Boolean): Unit = {
    val stops = StopWordsRemover.loadDefaultStopWords("english")

    val filterExp = if (topReviews) "stars > 3" else "stars < 4"
    val path = if (topReviews) "data/topReviews" else "data/worstReviews"

    val rdd = reviewDF.select("text", "stars")
      .filter(filterExp)
      .rdd
      .map(row => row(0).asInstanceOf[String].replaceAll("\\W+", " ").toLowerCase())
      .flatMap(_.split(" "))
      .filter(!stops.contains(_))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    sparkSession.createDataFrame(rdd)
      .toDF("word", "count")
      .limit(250)
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(path)
  }

  def userDetails(sparkSession: SparkSession, userDF: DataFrame): Unit = {
    val rdd = userDF.select("user_id", "review_count", "average_stars", "yelping_since", "friends")
      .rdd
      .filter(!_.anyNull)
      .map {
        row => (row(0).asInstanceOf[String],
                row(1).asInstanceOf[Long],
                row(2).asInstanceOf[Double],
                2019 - row(3).asInstanceOf[String].split(" ")(0).split("-")(0).toInt,
                row(4).asInstanceOf[String].split(",").length)

      }

    sparkSession.createDataFrame(rdd)
      .toDF("user_id", "review_count", "average_stars", "yelping_for", "friends")
      .coalesce(1)
      .write
      .option("header", "true")
      .csv("data/userDetails")
  }

}

