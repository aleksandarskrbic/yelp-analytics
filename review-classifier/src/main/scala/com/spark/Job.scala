package com.spark

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Binarizer, HashingTF, IDF, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Job {

  def main(args: Array[String]): Unit = {
    val spark = initSparkSession()

    val dataFrame = spark.read
      .format("json")
      .load(Config.path)

    describe(dataFrame)

    val Array(trainDF, testDF) = dataFrame.randomSplit(Array(0.8, 0.2), 42)

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

  def describe(dataFrame: DataFrame): Unit = {
    val binarizer = new Binarizer()
      .setThreshold(3)
      .setInputCol("stars")
      .setOutputCol("positive")

    val binarizered = binarizer.transform(dataFrame)

    val count = binarizered.count()
    println(s"Total reviews count: $count")

    binarizered.groupBy("positive").count().show()
  }

  def train(trainDF: DataFrame): PipelineModel = {
    val regexTokenizer = new RegexTokenizer()
      .setPattern("[\\W_]+")
      .setToLowercase(true)
      .setMinTokenLength(4)
      .setInputCol("text")
      .setOutputCol("tokenized")

    val stopWordsRemover = new StopWordsRemover()
      .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))
      .setInputCol("tokenized")

    val binarizer = new Binarizer()
      .setThreshold(3)
      .setInputCol("stars")
      .setOutputCol("positive")

    val tf = new HashingTF()
      .setInputCol("tokenized")
      .setOutputCol("tf_features")
      .setNumFeatures(5000)

    val idf = new IDF()
      .setInputCol("tf_features")
      .setOutputCol("features")
      .setMinDocFreq(5)

    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("positive")
      .setMaxIter(80)

    val pipeline = new Pipeline()
      .setStages(Array(regexTokenizer, stopWordsRemover, binarizer, tf, idf, lr))

    val model = pipeline.fit(trainDF)
    model.write.overwrite().save("spark-logistic-regression-model")

    model
  }

  def metrics(model: PipelineModel, testDF: DataFrame): Unit = {
    val out = model.transform(testDF)
      .select("prediction", "positive")
      .rdd
      .map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))

    val binaryClassificationMetrics = new BinaryClassificationMetrics(out)

    binaryClassificationMetrics.precisionByThreshold.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    binaryClassificationMetrics.recallByThreshold.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }
  }

}

