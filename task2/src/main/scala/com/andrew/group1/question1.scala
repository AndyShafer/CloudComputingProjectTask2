package org.andrew.task2.group1

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._

object Question1 {
  def main(args: Array[String]): Unit = {
    val appName = "Question1"
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val spark = SparkSession.builder().appName(appName).getOrCreate()

    val stream = ssc.textFileStream("s3://transportation-databases/sample")
    stream.count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
