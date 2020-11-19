package org.andrew.task2.group2

import org.apache.spark.sql.{ForeachWriter, Row}
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import java.util.ArrayList

import scala.collection.JavaConverters._

class Question1Writer extends ForeachWriter[Row] {
  private val tableName = "2_1"
  private val regionName = "us-east-1"
  private var currentAirport = ""
  private var carriers : Seq[String] = Seq()

  // This will lazily be initialized only when open() is called
  lazy val ddb = AmazonDynamoDBClientBuilder.standard()
    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
    .withRegion(regionName)
    .build()


  //
  // This is called first when preparing to send multiple rows.
  // Put all the initialization code inside open() so that a fresh
  // copy of this class is initialized in the executor where open()
  // will be called.
  //
  def open(partitionId: Long, epochId: Long) = {
    ddb  // force the initialization of the client
    true
  }

  def submit() = {
    if(currentAirport != "") {
      val rowMap = Map("airport" -> currentAirport, "top_10_carriers" -> carriers.toString)
      val dynamoItem = rowMap.mapValues {
        v: Any => new AttributeValue(v.toString)
      }.asJava

      ddb.putItem(tableName, dynamoItem)
    }
    currentAirport = ""
    carriers = Seq()
  }

  //
  // This is called for each row after open() has been called.
  // This implementation sends one row at a time.
  // A more efficient implementation can be to send batches of rows at a time.
  //
  def process(row: Row) = {
    val airport = row.getString(0)
    if(currentAirport == airport) {
      if(carriers.size < 10) {
        carriers = carriers :+ row.getString(1)
      }
    } else {
      submit()
      currentAirport = row.getString(0)
      carriers = Seq(row.getString(1))
    }
  }

  //
  // This is called after all the rows have been processed.
  //
  def close(errorOrNull: Throwable) = {
    submit()
    ddb.shutdown()
  }
}
