package com.datastax.migrator

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}


object DSEToAstraDBMigrator {

    //Then start spark session
  private def initSpark(): SparkSession = {
    SparkSession.builder().appName("Spark Job Migrating Data from DSE to Astra DB")
      .config("spark.sql.catalog.casscatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.output.ignoreNulls", true)
      .withExtensions(new CassandraSparkExtensions)
      .getOrCreate()
  }

  private def writeToDailyTbl(df: DataFrame, scb: String, host: String, clientid: String, tokenpwd: String): Unit = {

    val dfWithEventDay = df.withColumn("event_day",
      floor(unix_timestamp(col("latest_time")) / 86400))

    // Add a new column yyyy with just the year parsed from the event_day column
    val dfTsTable = dfWithEventDay.withColumn("yyyy", year(col("latest_time")))

    // write to insight_timeseries_daily in astra
    dfTsTable.write.format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.config.cloud.path", scb)
      .option("spark.cassandra.connection.host", host)
      .option("spark.cassandra.auth.username", clientid)
      .option("spark.cassandra.auth.password", tokenpwd)
      .options(Map(
        "keyspace" -> "insight",
        "table" -> "insight_timeseries_daily"
      ))
      .mode(SaveMode.Append)
      .save
  }

  private def writeToHourlyTbl(df: DataFrame, scb: String, host: String, clientid: String, tokenpwd: String): Unit = {
    // Convert the event_hour column to a date type
    val dfWithDate = df.withColumn("event_hour",
      format_string("%02d%02d%02d",
        year(col("latest_time")) - 2000,
        month(col("latest_time")),
        dayofmonth(col("latest_time"))
      ))

    // Add a new column yyyymm with yyyy and mm parsed from the event_hour column
    val dfTsTable = dfWithDate.withColumn("yyyymm", concat(year(col("latest_time")),
      format_string("%02d", month(col("latest_time")))))

    // write to insight_timeseries_hourly in astra
    dfTsTable.write.format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.config.cloud.path", scb)
      .option("spark.cassandra.connection.host", host)
      .option("spark.cassandra.auth.username", clientid)
      .option("spark.cassandra.auth.password", tokenpwd)
      .options(Map(
        "keyspace" -> "insight",
        "table" -> "insight_timeseries_hourly"
      ))
      .mode(SaveMode.Append)
      .save

  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = initSpark()

    // Specify your bucket
    val bucketPath = args(8)

    // Read all parquet
    val schema = StructType(List(StructField("tag_id", StringType, true)))

    // read recursively from the bucket
    val dfSource = spark.read.option("recursiveFileLookup", "true").schema(schema).parquet(bucketPath)

    val tagdf = dfSource.filter(col("tag_id").isNotNull)

    // load dataframe from insight_daily_ts
    val dailydf = spark.read.format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.host", args(0))
      .option("spark.cassandra.connection.port", args(1))
      .option("spark.cassandra.auth.username", args(2))
      .option("spark.cassandra.auth.password", args(3))
      .option("keyspace","insight_test").option("table", "insight_daily_ts").load()

    // Create a new DataFrame with only the tag_id column from tagdf
    val tagIdDF = tagdf.select("tag_id").distinct()

    // Perform an inner join on the tag_id column to get rows that are present in both DataFrames
    val resultdailyDF = dailydf.join(tagIdDF, "tag_id")

//    println("Daily count: " + dailydf.count())
//    println("Filtered Daily count: " + resultdailyDF.count())

    val scb = args(4)
    val host = args(5)
    val clientid = args(6)
    val tokenpwd = args(7)

    // write to the daily table
    writeToDailyTbl(resultdailyDF, scb, host, clientid, tokenpwd)

    // load dataframe from insight_hourly_ts
    val hourlydf = spark.read.format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.host", args(0))
      .option("spark.cassandra.connection.port", args(1))
      .option("spark.cassandra.auth.username", args(2))
      .option("spark.cassandra.auth.password", args(3))
      .option("keyspace", "insight_test").option("table", "insight_hourly_ts").load()

//    println("Hourly count: " + hourlydf.count())

    // Perform an inner join on the tag_id column to get rows that are present in both DataFrames
    val resulthourlyDF = hourlydf.join(tagIdDF, "tag_id")
    // write to the hourly table
    writeToHourlyTbl(resulthourlyDF, scb, host, clientid, tokenpwd)

    spark.stop()
  }
}

