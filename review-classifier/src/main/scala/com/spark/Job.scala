package com.spark

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{Binarizer, HashingTF, IDF, RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics, MultilabelMetrics}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Job {

  def main(args: Array[String]): Unit = {
    val path = Config.path
    val spark = initSparkSession()
    val df = loadData(spark, path)

    //print statistics about data sets and class distributions

    val tokenizer = new RegexTokenizer()
      .setPattern("[\\W_]+")
      .setToLowercase(true)
      .setMinTokenLength(4)
      .setInputCol("text")
      .setOutputCol("tokenized")

    val tokenizedDF = tokenizer.transform(df)

    val englishStopWords = StopWordsRemover.loadDefaultStopWords("english")
    val stops = new StopWordsRemover()
      .setStopWords(englishStopWords)
      .setInputCol("tokenized")

    val cleanedDF = stops.transform(tokenizedDF)

    val tf = new HashingTF()
      .setInputCol("tokenized")
      .setOutputCol("TFOut")
      .setNumFeatures(1000)

    val idf = new IDF()
      .setInputCol("TFOut")
      .setOutputCol("IDFOut")
      .setMinDocFreq(2)

    val tfIdf = idf.fit(tf.transform(cleanedDF)).transform(tf.transform(cleanedDF))

    val lr = new LogisticRegression()
      .setFeaturesCol("IDFOut")
      .setLabelCol("positive")
      .setMaxIter(10)

    val Array(train, test) = tfIdf.randomSplit(Array(0.7, 0.3), 42)

    val fittedLR = lr.fit(train)

    val out = fittedLR.transform(test)
      .select("prediction", "positive")
      .rdd
      .map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))

    val metrics = new BinaryClassificationMetrics(out)

    metrics.precisionByThreshold.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    metrics.recallByThreshold.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

  }

  def initSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(Config.appName)
      .master(Config.master)
      .getOrCreate()

    spark
  }

  def loadData(spark: SparkSession, path: String): DataFrame = {
    val rawDF = spark.read
      .format("json")
      .load(path)

    val binarizer = new Binarizer()
      .setThreshold(3)
      .setInputCol("stars")
      .setOutputCol("positive")

    val binarizedDF = binarizer.transform(rawDF)
    val finalDF = binarizedDF.select("text", "positive")

    finalDF
  }

}
