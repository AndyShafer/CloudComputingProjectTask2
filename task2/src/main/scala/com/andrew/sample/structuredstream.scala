package org.andrew.task2.sample

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._

object StructuredStream {
  def main(args: Array[String]): Unit = {
    val appName = "StructuredStream"
    val spark = SparkSession.builder().appName(appName).getOrCreate()

    import spark.implicits._

    val rows = spark.readStream.text("s3://transportation-databases/sample")

    val rowCounts = rows.groupBy("value").count()
    
    val query = rowCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }
}
