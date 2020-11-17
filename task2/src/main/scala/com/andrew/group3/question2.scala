package org.andrew.task2.group3

import java.lang.RuntimeException

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{date_sub,concat,lit}

object Question2 {
  def main(args: Array[String]): Unit = {
    if(args.size < 3) {
      throw new RuntimeException("Missing command line parameter")
    }
    val startAirport = args(0)
    val intermediateAirport = args(1)
    val endAirport = args(2)
    val appName = "Question2"
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
     
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val rows = spark.readStream.option("header", "true").schema(onTimeSchema).csv("s3://transportation-databases/airline_ontime")

    val selection = rows.select($"Carrier", $"Origin", $"Dest", $"ArrDelay", $"FlightDate", $"DepTime").filter(row => row.getAs("ArrDelay") != null)

    val cityA = selection.select($"Carrier".as("carrier1"), $"Origin".as("airport_a"), $"Dest".as("airport_b1"), $"ArrDelay".cast(LongType).as("delay_1"), $"FlightDate".cast(DateType).as("start_date1"), $"DepTime".as("dep_time1")).filter($"dep_time1".cast(LongType) < 1200)

    val cityB = selection.select($"Carrier".as("carrier2"), $"Origin".as("airport_b2"), $"Dest".as("airport_c"), $"ArrDelay".cast(LongType).as("delay_2"), date_sub($"FlightDate".cast(DateType), 2).as("start_date2"), $"DepTime".as("dep_time2")).filter($"dep_time2".cast(LongType) > 1200)

    val trip = cityA.join(cityB, $"airport_b1" === $"airport_b2" && $"start_date1" === $"start_date2")
      .filter(row => row.getAs("airport_a") == startAirport && row.getAs("airport_b1") == intermediateAirport && row.getAs("airport_c") == endAirport)
      .select(concat($"airport_a", lit(","), $"airport_b1", lit(","), $"airport_c", lit(","), $"start_date1").as("trip"), ($"delay_1" + $"delay_2").as("total_delay"), $"carrier1", $"dep_time1", $"carrier2", $"dep_time2")

    val query = trip.writeStream.foreach(new Question2Writer)start()
    query.awaitTermination()
  }
}
