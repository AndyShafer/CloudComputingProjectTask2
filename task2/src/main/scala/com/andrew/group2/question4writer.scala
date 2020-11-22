package org.andrew.task2.group2

import org.apache.spark.sql.{ForeachWriter, Row}
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.TableWriteItems
import java.util.ArrayList

import scala.collection.JavaConverters._

class Question4Writer extends ForeachWriter[Row] {
  private val tableName = "2_4"
  private val regionName = "us-east-1"
  private val batchSize = 10
  private var items : Seq[java.util.Map[String,AttributeValue]] = Seq()

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
