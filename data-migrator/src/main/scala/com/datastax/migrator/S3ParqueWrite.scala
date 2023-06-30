package com.datastax.migrator

import org.apache.spark.sql.SparkSession

import java.sql.Timestamp

object S3ParqueWrite {

  private def initSpark(): SparkSession = {
    //Then start spark session
    SparkSession.builder
      .master("local")
      .appName("scala-s3")
      .getOrCreate()
  }

  case class EventTs(tag_id: String,  event_time: java.sql.Timestamp,data_quality: Int, event_value: Double)

  def main(args: Array[String]): Unit = {

    val sparkSession = initSpark()

    val sc = sparkSession.sparkContext

    // credential
    sc.hadoopConfiguration.set("fs.s3a.access.key", "ASIAVGQJT4RYHQOUWLLV")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "oFxq3LnQZLjUJpzjCvhG8WSbs+ZyK/mcH0j0eziY")

    // add the nex two lines if key and secret are temporary
    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    sc.hadoopConfiguration.set("fs.s3a.session.token","IQoJb3JpZ2luX2VjEEQaCXVzLXdlc3QtMiJHMEUCIDs9eFYHcp+okZpJ6r7w1cslDj4WAdqJRTr5VHw2ROlRAiEAu8nABVgKTWWy65NokAaHx1Gtsf7U2JCQEeLUL3IZT8cqqQMIrf//////////ARADGgwzNTc1NzYyMDU0MjQiDM91ElaXIB/QhsBovSr9Ape1whVt4ziOjyDcyC6SygiBiN278LpcE2DSAwFZ46BKF6XyXrWYM7Uz/f/Trz7BStuCc6iDGYmdIDks+02YUi2HZcVUIl1+FHzGM6iNFMvUgQa8bgQlqHH95y1THCmq65iwE+YZGuOPUJW0zE0TOxAElF5XN5lj/Y/jaggXsviEvbEU0s7PIoTYCR0zR0s6DULsuluK3mlueh4HEZZNDEtVP1VtGzY4w1jIb/MZMPiPlYFDvscxYv/ZyIdli+5794ehOG0hTdbeO1xxRbCXoBI/n6fs3yn+JXFq5894QTBWgC4EvqfTR2vpKzjSgHrFl3s1SYVjcbfU9aTk4JcIISvKOFLOqUlzM/tWPGlPDQKPYH2v0SPqJkNqJ2Spzx3nKFQ8L+WxTlj7gEE4XpAz6cPnbY+OpP8ej01SqTuuT4a/OnXzuqiW9/UDGg/mHOolzPrwqBAjavsDVntO9xseb7irIMLmNhlto5YJpu4S0x4UUryIhRXPLWlRudeGFzDa2fykBjqmAZr1r6AUnXq9RfO9bLrT3nYQ3Aps0T8InU3wwog/NOZ77zv9Iy/Rq9FM8QH5iFwD438JVdlfpGy2/zZ+OO8vedAP3zDvCxGx7TxSSzX+FPq3n5FW4/Rj4mppoXkdOIHMY18DHVJ/ffqC+agQzvHIjG5HBP69UbeCcUENUOqv+qJS/9CQSarJ9/amX8ylTJf8NCuarZS1Vg5OGdQWFeBGpt2uhWjnuu8=")




    val s3BucketName = "my-emr-s3-test"
    val outputPath = "s3a://" + s3BucketName + "/suezsample1.parquet"

    val events1 = (1 to 3).map(i => EventTs("mno.1",  Timestamp.valueOf(s"2023-06-28 00:00:0$i"),1, i.toDouble))
    val events2 = (1 to 3).map(i => EventTs("mno.1",  Timestamp.valueOf(s"2023-06-28 00:00:0$i"),0, i.toDouble))
    val events3 = (1 to 3).map(i => EventTs("mno.1",  Timestamp.valueOf(s"2023-06-28 00:00:0$i"),3 ,i.toDouble))
    val events4 = (1 to 3).map(i => EventTs("mno.1.uuuu",  Timestamp.valueOf(s"2023-06-28 00:00:0$i"), 1,i.toDouble))

    val df1 = sparkSession.createDataFrame(events1)
    val df2 = sparkSession.createDataFrame(events2)
    val df3 = sparkSession.createDataFrame(events3)
    val df4 = sparkSession.createDataFrame(events4)
    val df = df1.union(df2).union(df3).union(df4)

    df.show()
    df.write.parquet(outputPath)
    sys.exit(0)
  }
}

