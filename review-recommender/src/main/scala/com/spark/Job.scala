package com.spark

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}

object Job {

  def main(args: Array[String]): Unit = {
    val spark = initSparkSession()

    val dataFrame = spark.read
      .format("json")
      .load(Config.path)
      .select("user_id", "business_id", "stars")

    val Array(trainDF, testDF) = dataFrame.randomSplit(Array(0.8, 0.2))

    val model = train(trainDF)

    metrics(model, testDF)
  }

  def initSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(Config.appName)
      .master(Config.master)
      .getOrCreate()

    spark
  }

  def train(trainDF: DataFrame): PipelineModel = {
    val userIndexer = new StringIndexer()
      .setHandleInvalid("keep")
      .setInputCol("user_id")
      .setOutputCol("userId")

    val businessIndexer = new StringIndexer()
      .setHandleInvalid("keep")
      .setInputCol("business_id")
      .setOutputCol("businessId")

    val als = new ALS()
      .setMaxIter(8)
      .setRegParam(0.2)
      .setNonnegative(true)
      .setColdStartStrategy("drop")
      .setUserCol("userId")
      .setItemCol("businessId")
      .setRatingCol("stars")

    val pipeline = new Pipeline()
      .setStages(Array(userIndexer, businessIndexer, als))

    val model = pipeline.fit(trainDF)
    model.write.overwrite().save("spark-als-model")

    model
  }

  def metrics(model: PipelineModel, testDF: DataFrame): Unit = {
    val predictions = model.transform(testDF)

    val rmse = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("stars")
      .setPredictionCol("prediction")

    val r2 = new RegressionEvaluator()
      .setMetricName("r2")
      .setLabelCol("stars")
      .setPredictionCol("prediction")

    println(s"RMSE = ${rmse.evaluate(predictions)}")
    println(s"R2 = ${r2.evaluate(predictions)}")
  }

}

