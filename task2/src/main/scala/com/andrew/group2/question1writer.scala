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

  // This will lazily be initialized only when open() is called
  lazy val ddb = AmazonDynamoDBClientBuilder.standard()
    .withCredentials(new ProfileCredentialsProvider())
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

  //
  // This is called for each row after open() has been called.
  // This implementation sends one row at a time.
  // A more efficient implementation can be to send batches of rows at a time.
  //
  def process(row: Row) = {
    val rowAsMap = row.getValuesMap(row.schema.fieldNames)
    val dynamoItem = rowAsMap.mapValues {
      v: Any => new AttributeValue(v.toString)
    }.asJava

    ddb.putItem(tableName, dynamoItem)
  }

  //
  // This is called after all the rows have been processed.
  //
  def close(errorOrNull: Throwable) = {
    ddb.shutdown()
  }
}
