package org.andrew.task2.group1

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Question1 {
  def main(args: Array[String]): Unit = {
    val appName = "Question1"
    val onTimeSchema = new StructType()
      .add("Year", StringType)
      .add("Quarter", StringType)
      .add("Month", StringType)
      .add("DayofMonth", StringType)
      .add("DayOfWeek", StringType)
      .add("FlightDate", StringType)
      .add("UniqueCarrier", StringType)
      .add("AirlineID", IntegerType)
      .add("Carrier", StringType)
      .add("TailNum", StringType)
      .add("FlightNum", StringType)
      .add("Origin", StringType)
      .add("OriginCityName", StringType)
      .add("OriginState", StringType)
      .add("OriginStateFips", StringType)
      .add("OriginStateName", StringType)
      .add("OriginWac", StringType)
      .add("Dest", StringType)
      .add("DestCityName", StringType)
      .add("DestState", StringType)
      .add("DestStateFips", StringType)
      .add("DestStateName", StringType)
      .add("DestWac", StringType)
      .add("CRSDepTime", StringType)
      .add("DepTime", StringType)
      .add("DepDelay", StringType)
      .add("DepDelayMinutes", StringType)
      .add("DepDel15", StringType)
      .add("DepartureDelayGroups", StringType)
      .add("DepTimeBlk", StringType)
      .add("TaxiOut", StringType)
      .add("WheelsOff", StringType)
      .add("WheelsOn", StringType)
      .add("TaxiIn", StringType)
      .add("CRSArrTime", StringType)
      .add("ArrTime", StringType)
      .add("ArrDelay", StringType)
      .add("ArrDelayMinutes", StringType)
      .add("ArrDel15", StringType)
      .add("ArrivalDelayGroups", StringType)
      .add("ArrTimeBlk", StringType)
      .add("Cancelled", StringType)
      .add("CancellationCode", StringType)
      .add("Diverted", StringType)
      .add("CRSElapsedTime", StringType)
      .add("ActualElapsedTime", StringType)
      .add("AirTime", StringType)
      .add("Flights", StringType)
      .add("Distance", StringType)
      .add("DistanceGroup", StringType)
      .add("CarrierDelay", StringType)
      .add("WeatherDelay", StringType)
      .add("NASDelay", StringType)
      .add("SecurityDelay", StringType)
      .add("LateAircraftDelay", StringType)
      .add("FirstDepTime", StringType)
      .add("TotalAddGTime", StringType)
      .add("LongestAddGTime", StringType)
      .add("DivAirportLandings", StringType)
      .add("DivReachedDest", StringType)
      .add("DivActualElapsedTime", StringType)
      .add("DivArrDelay", StringType)
      .add("DivDistance", StringType)
      .add("Div1Airport", StringType)
      .add("Div1WheelsOn", StringType)
      .add("Div1TotalGTime", StringType)
      .add("Div1LongestGTime", StringType)
      .add("Div1WheelsOff", StringType)
      .add("Div1TailNum", StringType)
      .add("Div2Airport", StringType)
      .add("Div2WheelsOn", StringType)
      .add("Div2TotalGTime", StringType)
      .add("Div2LongestGTime", StringType)
      .add("Div2WheelsOff", StringType)
      .add("Div2TailNum", StringType)
      .add("", StringType)
     
    //val sparkConf = new SparkConf().setAppName(appName)
    //val ssc = new StreamingContext(sparkConf, Seconds(10))
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val rows = spark.readStream.option("header", "true").schema(onTimeSchema).csv("s3://transportation-databases/airline_ontime")

    val airports = rows.select($"Origin", $"Dest")
    airports.printSchema()

    val flights = airports.flatMap(row => Seq((row.getString(0), 1), (row.getString(1), 1))).groupBy("_1").sum("_2").sort($"_1".desc).limit(10)
    flights.printSchema()

    val query = flights.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }
}
