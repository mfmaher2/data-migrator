package com.datastax.migrator

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max, to_timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
object FunctionalHelper {

  def getMaxTimeFromStringType(columnName: String, tableName: String, keyspace: String, spark: SparkSession): Long = {
    val df = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keyspace))
      .load()

    // Assuming event_time is in format "yyyy-MM-dd HH:mm:ss.SSS"
    val maxTime = df
      .withColumn("parsed_time", to_timestamp(col(columnName), "yyyy-MM-dd HH:mm:ss.SSS"))
      .agg(max("parsed_time").as("max_time"))
      .collect()(0)
      .getAs[java.sql.Timestamp]("max_time")

    maxTime.getTime // Convert to milliseconds since epoch
  }

  def getLimitTimestamp(): Long = {
    System.currentTimeMillis() - 24 * 60 * 60 * 1000 // Current time minus 24 hours in milliseconds
  }
//  def getLimitTimestamp(): String = {
//    val limitTime = LocalDateTime.now().minusHours(24) // Adjust the period as needed
//    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//    limitTime.format(formatter)
//  }
}
