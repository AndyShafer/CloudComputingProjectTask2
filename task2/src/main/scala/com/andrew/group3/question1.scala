package org.andrew.task2.group3

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Question1 {
  def main(args: Array[String]): Unit = {
    val appName = "Question1"
    val airCarrierSchema = new StructType()
      .add("PASSENGERS", StringType)
      .add("FREIGHT", StringType)
      .add("MAIL", StringType)
      .add("DISTANCE", StringType)
      .add("UNIQUE_CARRIER", StringType)
      .add("AIRLINE_ID", StringType)
      .add("UNIQUE_CARRIER_NAME", StringType)
      .add("UNIQUE_CARRIER_ENTITY", StringType)
      .add("REGION", StringType)
      .add("CARRIER", StringType)
      .add("CARRIER_NAME", StringType)
      .add("CARRIER_GROUP", StringType)
      .add("CARRIER_GROUP_NEW", StringType)
      .add("ORIGIN", StringType)
      .add("ORIGIN_CITY_NAME", StringType)
      .add("ORIGIN_CITY_NUM", StringType)
      .add("ORIGIN_STATE_ABR", StringType)
      .add("ORIGIN_STATE_FIPS", StringType)
      .add("ORIGIN_STATE_NM", StringType)
      .add("ORIGIN_WAC", StringType)
      .add("DEST", StringType)
      .add("DEST_CITY_NAME", StringType)
      .add("DEST_CITY_NUM", StringType)
      .add("DEST_STATE_ABR", StringType)
      .add("DEST_STATE_FIPS", StringType)
      .add("DEST_STATE_NM", StringType)
      .add("DEST_WAC", StringType)
      .add("YEAR", StringType)
      .add("QUARTER", StringType)
      .add("MONTH", StringType)
      .add("DISTANCE_GROUP", StringType)
      .add("CLASS", StringType)
     
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val rows = spark.readStream.option("header", "true").schema(airCarrierSchema).csv("s3://transportation-databases/air_carrier_statistics_ALL")

    val selection = rows.select($"ORIGIN", $"DEST", $"PASSENGERS")

    val flights = selection.flatMap(row => Seq((row.getString(0), row.getString(2).toDouble.toLong, (row.getString(1), row.getString(2).toDouble.toLong)))).groupBy("_1").sum("_2").sort($"sum(_2)".desc)

    val query = flights.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }
}
