package org.andrew.task2.group2

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{concat, lit}

object Question4 {
  def main(args: Array[String]): Unit = {
    val appName = "Question4"
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

    val rows = spark.readStream.option("header", "true").schema(onTimeSchema).csv("s3://transportation-databases/streaming_data")

    val selection = rows.select($"Origin", $"Dest", $"DepDelay".cast(DoubleType))
      .filter(row => row.getAs("DepDelay") != null)

    val meanDelay = selection.groupBy($"Origin", $"Dest").mean("DepDelay")

    val concatAirports = meanDelay.select(concat($"Origin", lit(","), $"Dest").as("trip"), $"avg(DepDelay)".as("average_delay"))

    val query = concatAirports.writeStream.outputMode("complete").foreach(new Question4Writer).start()
    query.awaitTermination()
  }
}
