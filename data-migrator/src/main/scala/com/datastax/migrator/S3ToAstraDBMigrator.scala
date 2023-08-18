package com.datastax.migrator

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, concat, instr, month, rank, row_number, substring, substring_index, year}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}


object S3ToAstraDBMigrator {
  private def initSpark(): SparkSession = {

    //Then start spark session
    SparkSession.builder().appName("Spark Job Migrating Data from S3 to Astra DB")
      .config("spark.sql.catalog.casscatalog", "com.com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.output.ignoreNulls", true)
      .withExtensions(new CassandraSparkExtensions).getOrCreate()
  }

  private def writeToCurrentValueTbl(df: DataFrame): Unit = {
    // only keep data_quality != 0
    var dfCurrentValue = df.filter(col("data_quality") =!= 0)
    dfCurrentValue = dfCurrentValue.withColumn("asset_id",
      substring_index(col("tag_id"),".",1))
    dfCurrentValue = dfCurrentValue.drop("data_quality")

    //current_Value before write
    dfCurrentValue.write.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> "insight",
        "table" -> "current_value"
      ))
      .mode(SaveMode.Append)
      .save
  }

  private def writeTimeSeriesTbl(df: DataFrame): Unit = {
    // Add a new column yyyymm with yyyy and mm parsed from the event_time column
    val dfTsTable = df.withColumn("yyyymm", concat(year(col("event_time")),
      functions.format_string("%02d", month(col("event_time")))))

    // change the table name
    dfTsTable.write.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> "insight",
        "table" -> "timeseries_raw"
      ))
      .mode(SaveMode.Append)
      .save
  }
  def main(args: Array[String]): Unit = {
   val spark: SparkSession = initSpark


    // Specify your bucket
    val bucketPath = args(0)

    // Read all parquet
    // Define the schema
    val schema = StructType(
      List(
        StructField("tag_id", StringType, true),
        StructField("data_quality", IntegerType, true),
        StructField("event_time", TimestampType, true),
        StructField("event_value", DoubleType, true)
      )
    )
    // read recursively from the bucket
    val dfSource = spark.read.option("recursiveFileLookup", "true").schema(schema).parquet(bucketPath)

    // Check and log records with null tag_id
    val nullTagIdRecords = dfSource.filter(col("tag_id").isNull)
    if (nullTagIdRecords.count > 0) {
      println("Records with null tag_id:")
      nullTagIdRecords.show()
    } else {
      println("No records with null tag_id found.")
    }

    // repartition the df with tag_id and data_quality
    val windowSpec = Window.partitionBy("tag_id",
      "data_quality").orderBy(col("event_time").desc)

    val dfTs = dfSource.withColumn("row_number",row_number().over(windowSpec))
      .filter(col("row_number") === 1).drop("row_number")
    // write to the time series table
    writeTimeSeriesTbl(dfTs)
    // write to the current value table
    writeToCurrentValueTbl(dfTs)
    spark.stop()
  }
}

