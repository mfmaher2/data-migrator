package com.datastax.migrator

import org.apache.spark.sql.SparkSession

object S3Read {

  private def initSpark(): SparkSession = {
    //Then start spark session
    SparkSession.builder
      .master("local")
      .appName("scala-s3")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {

    val sparkSession = initSpark()
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext

    // credential
    sc.hadoopConfiguration.set("fs.s3a.access.key", "ASIAVGQJT4RYG75WQZET")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "vHhaFpyyCMoack0g2XO88myBtihfxLIvciLi2aHv")

    // add the nex two lines if key and secret are temporary
    sc.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    sc.hadoopConfiguration.set("fs.s3a.session.token", "IQoJb3JpZ2luX2VjEGMaCXVzLXdlc3QtMiJIMEYCIQC2EiDfz973pLq3EisgyPiFWxcVjPLKAryyyut2K0ffmgIhAIZv886VmppW+5I4e09K75/ECjcjs1D89xpFS3T1NI5ZKqgDCLz//////////wEQAxoMMzU3NTc2MjA1NDI0IgxCfmMMx03z4BTdKrAq/AIjfJgJxCiCQncgU0RKD2/HGzUoRYadlnMfh5fTMi68uVgBc2g7yqTrcn24oVywPF4iH255HzykZgkTZfWhVBdeQzyvi1TWNrai0hfhwRK381a9XtFeeN1lzPUmbXHACcanke8sSiOOHbKxWaykLpn0+p79ZSuuCYRb1dh4LBvKInB0u3vmdbyo3Zhmn6hvEslLF+L7NtOjZ6QYzwz86opKMI/0PEDxzfp39XLem5SZR3TbmY24nKtMhpLvm/uJwNZlS+Ez4vJLGaeM3FfAh3qx4/4f6aM9wOEBf/1VGLUBmnOjPYs46UDgojgGCONxNBMdw32Q8X5fBOxur4iWQQQVdGCyyu3xAaj+yWEx6I10ZvWOTQao8DqLYRswf+pRjcADKdRgqQMmSEvjC/vbsAPuG/EX7hHakyIaftLrEA+5rgD+2TJiHCVYm3nvdGH0CldSEIWmEcrGQXQh4gqbiDltMJ5bVDze1+ztmjvJaCj9UsuBOHZ/H6oDmx+DHTC3t8ukBjqlAYyDApkOvmlTUKso0P/IEOo03mrCAWGOymSgJnlhq4xnuWNgMKB9pdPYTRv8J78b1izd62IwviZOYqcBRW/KoxYmeztjLVE7DOBSAvSanHb91g/dCYBQa4hyKw5rYmkP9fwpa3S//zUZLbMPUKCHxUdqc2IRKghoLJqI4zLK1f5apDPUi9aFtJD03ApstZpBFSWd40ZwPqSCs98woNXEKWJAdAdfMg==")




    val s3BucketName = "collector-dead-drop"
    val outputPath = "s3a://" + s3BucketName + "/ADH/output2.parquet"

    val columns = Seq("language", "likes")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val df = data.toDF(columns:_*)
    df.show()
    df.write.parquet(outputPath)
    sys.exit(0)
  }
}

