package com.datastax.migrator

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, concat, month, year}


object S3ToAstraDBMigrator {
  private def initSpark(): SparkSession = {

    //Then start spark session
    SparkSession.builder().appName("Spark Job Migrating Data from S3 to Astra DB")
      .master("local[*]")
      .config("spark.cassandra.connection.config.cloud.path", "./secure-connect-vector-test.zip")
      .config("spark.sql.catalog.casscatalog", "com.com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.output.ignoreNulls", true)
      .withExtensions(new CassandraSparkExtensions).getOrCreate()
  }

  private def initS3Access(spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    // credential (need to use new session key and secret since they are transient
    sc.hadoopConfiguration.set("fs.s3a.access.key", "you-key")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "your-secret")

    // the next two line only needed if key and secret are temporary
    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    sc.hadoopConfiguration.set("fs.s3a.session.token", "your-token")
  }
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = initSpark()
    initS3Access(spark)

    // Specify your bucket
    val bucketPath = "s3a://my-emr-s3-test/"

    // Read all CSV files
    var df = spark.read.format("csv")
      .option("header", "true")
      .option("recursiveFileLookup", "true")
      .load(bucketPath)

    // Add a new column yyyymm with yyyy and mm parsed from the event_time column
    df = df.withColumn("yyyymm", concat(year(col("event_time")),
      functions.format_string("%02d", month(col("event_time")))))

    df.write.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> "insight",
        "table" -> "insight_ts_new"
      ))
      .mode(SaveMode.Append)
      .save
    spark.stop()
  }
}

