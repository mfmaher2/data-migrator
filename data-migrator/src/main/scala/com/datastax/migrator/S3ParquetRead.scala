package com.datastax.migrator

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp

object S3ParquetRead {

  private def initSpark(): SparkSession = {
    //Then start spark session
    SparkSession.builder
      .master("local")
      .appName("scala-s3")
      .getOrCreate()
  }

  case class EventTs(tag_id: String, data_quality: Int, event_time: java.sql.Timestamp, event_value: Double)

  def main(args: Array[String]): Unit = {

    val sparkSession = initSpark()
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext

    // credential
    sc.hadoopConfiguration.set("fs.s3a.access.key", "ASIAVGQJT4RYBYLCC54L")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "IEa7LEv3Kv7rdM74fA9RLp3EYXoHj6Ghok3PZyEb")

    // add the nex two lines if key and secret are temporary
    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    sc.hadoopConfiguration.set("fs.s3a.session.token", "IQoJb3JpZ2luX2VjECUaCXVzLXdlc3QtMiJHMEUCIQDC3d260WcCd/zRxfac3m/xas2qQEddQt61DiIFB48mzwIgeauKRolK8IPrZ4ZOzOIY3MSuA3pfJEIeRC4PHWZ55VwqqQMIjv//////////ARADGgwzNTc1NzYyMDU0MjQiDByZvG552rd4MmKnlSr9AjIdOXMOJ4f329nnUiva6wG4GRyZPZitSwPD/Z23ItolbYwLfhS8aLYrk48Uz8LRnRVtVV1o1FmF642toHI9HwOUIgy52Bd8kOjOI1G3TAQHvY44jLmjJ0Kg2AF5WmPj16y12tTW7d76se+C61a2j6+gJdAvT2L03MioAZhuLUPN8CdCMZ+B4xrzn6voyIT+rv39TjNShvnMdVZ54JVu1gdj7VI0faeziVO+kXObmLQUJD+w/6nEsyw+bdVE3pwBtXT24pooanwORUIRT0Dsaql0BodN9zAzNa24Dz0B7/po5g+FDBSzouvVGpO6mr9t7KxKAGbn/MA9t/OVJcHwBtgmwu+Xg5yL35voLG+Am6BZ96gza4lF9vzTc9S7XoExHGRnwOZ4sH/aTSYDDCvW2AIVhSNNykkBaIKsnwtfAaEE6y40S5i6Oh2PBt4NF2sht2E3vin0sgry8ibQkxmBsioM+nYBrWE9NCVrT5/DrVSzKuuqmNYZakmnqY9U/TDc9/WkBjqmAfVXL65bNUKTgud7EA6RmuIq5gmgHTi1fDyNtfr/uU9TCEt3raVD98Ionu0h53K3ehLVZ2yDyxb51eTEvA+2fuKW2PmrgPBEOZIWxPaciiiiLomGoAn5ccI/v6r1AAh9w6ELlUEeZw68UPXdWWEv7ifJyJopbF0rNZrbXbETsxoiO8rf0pI8m9+Uh7bqtOqnxt0a7BsdRPgzYgscEN6SDQdrTpsF764=")




    val s3BucketName = "myparquetfiles1"
    val readPath = "s3a://" + s3BucketName + "/"

    // Define the schema
    val schema = StructType(
      List(
        StructField("tag_id", StringType, true),
        StructField("data_quality", IntegerType, true),
        StructField("event_time", TimestampType, true),
        StructField("event_value", DoubleType, true)
      )
    )



    var df = sparkSession.read.option("recursiveFileLookup", "true").schema(schema).parquet(readPath)

    df.show
    println("count"+ df.count)
    sys.exit(0)
  }
}

