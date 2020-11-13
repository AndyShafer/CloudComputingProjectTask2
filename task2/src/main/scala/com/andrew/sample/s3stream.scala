package org.andrew.task2.sample

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object S3Stream {
  def main(args: Array[String]): Unit = {
    val appName = "S3Stream"
    val sparkConf = new SparkConf().setAppName(appName)
    // Streams data and shows results at 10 second intervals.
    val ssc = new StreamingContext(sparkConf, Seconds(10))


    // Stream files that are placed into the S3 directory. Will not stream files that are already there.
    val stream = ssc.textFileStream("s3://transportation-databases/sample")
    // Pring the number of lines that were streamed during the interval.
    stream.count().print()

    // Start the stream.
    ssc.start()
    ssc.awaitTermination()
  }
}
