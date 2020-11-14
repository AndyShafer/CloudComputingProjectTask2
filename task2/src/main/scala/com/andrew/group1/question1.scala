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
      .add("Year", IntegerType)
      .add("Quarter", IntegerType)
      .add("Month", IntegerType)
      .add("DayofMonth", IntegerType)
      .add("DayOfWeek", IntegerType)
      .add("FlightDate", DateType)
      .add("UniqueCarrier", StringType)
      .add("AirlineID", IntegerType)
      .add("Carrier", StringType)
      .add("TailNum", IntegerType)
      .add("FlightNum", IntegerType)
      .add("Origin", StringType)
      .add("OriginCityName", StringType)
      .add("OriginState", StringType)
      .add("OriginStateFips", IntegerType)
      .add("OriginStateName", StringType)
      .add("OriginWac", IntegerType)
      .add("Dest", StringType)
      .add("DestCityName", StringType)
      .add("DestState", StringType)
      .add("DestStateFips", IntegerType)
      .add("DestStateName", StringType)
      .add("DestWac", IntegerType)
      .add("CRSDepTime", IntegerType)
      .add("DepTime", IntegerType)
      .add("DepDelay", IntegerType)
      .add("DepDelayMinutes", IntegerType)
      .add("DepDel15", IntegerType)
      .add("DepartureDelayGroups", IntegerType)
      .add("DepTimeBlk", StringType)
      .add("TaxiOut", StringType)
      .add("WheelsOff", StringType)
      .add("WheelsOn", StringType)
      .add("TaxiIn", StringType)
      .add("CRSArrTime", IntegerType)
      .add("ArrTime", IntegerType)
      .add("ArrDelay", IntegerType)
      .add("ArrDelayMinutes", IntegerType)
      .add("ArrDel15", IntegerType)
      .add("ArrivalDelayGroups", IntegerType)
      .add("ArrTimeBlk", StringType)
      .add("Cancelled", IntegerType)
      .add("CancellationCode", StringType)
      .add("Diverted", IntegerType)
      .add("CRSElapsedTime", IntegerType)
      .add("ActualElapsedTime", IntegerType)
      .add("AirTime", IntegerType)
      .add("Flights", IntegerType)
      .add("Distance", IntegerType)
      .add("DistanceGroup", IntegerType)
      .add("CarrierDelay", IntegerType)
      .add("WeatherDelay", IntegerType)
      .add("NASDelay", IntegerType)
      .add("SecurityDelay", IntegerType)
      .add("LateAircraftDelay", IntegerType)
      .add("FirstDepTime", IntegerType)
      .add("TotalAddGTime", IntegerType)
      .add("LongestAddGTime", IntegerType)
      .add("DivAirportLandings", IntegerType)
      .add("DivReachedDest", IntegerType)
      .add("DivActualElapsedTime", IntegerType)
      .add("DivArrDelay", IntegerType)
      .add("DivDistance", IntegerType)
      .add("Div1Airport", IntegerType)
      .add("Div1WheelsOn", IntegerType)
      .add("Div1TotalGTime", IntegerType)
      .add("Div1LongestGTime", IntegerType)
      .add("Div1WheelsOff", IntegerType)
      .add("Div1TailNum", IntegerType)
      .add("Div2Airport", IntegerType)
      .add("Div2WheelsOn", IntegerType)
      .add("Div2TotalGTime", IntegerType)
      .add("Div2LongestGTime", IntegerType)
      .add("Div2WheelsOff", IntegerType)
      .add("Div2TailNum", IntegerType)
      .add("", IntegerType)
      
    //val sparkConf = new SparkConf().setAppName(appName)
    //val ssc = new StreamingContext(sparkConf, Seconds(10))
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val rows = spark.readStream.option("header", "true").schema(onTimeSchema).csv("s3://transportation-databases/airline_ontime")

    val airports = rows.select($"Origin", $"Dest")
    airports.printSchema()

    val flights = airports.flatMap(row => Seq((row.getString(0), 1), (row.getString(1), 1))).groupBy("_1").sum("_2")
    flights.printSchema()

    val query = flights.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()
  }
}
