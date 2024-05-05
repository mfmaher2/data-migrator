package com.datastax.migrator

import com.datastax.spark.connector.CassandraSparkExtensions
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, functions}
import org.joda.time.DateTime

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

object AstraToS3DBMigrator {
  private def initSpark(): SparkSession = {

    //Then start spark session
    SparkSession.builder().appName("Spark Job Migrating Data from Astra DB to S3")
      .config("spark.sql.catalog.casscatalog", "com.com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.output.ignoreNulls", true)
      .withExtensions(new CassandraSparkExtensions).getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = initSpark


    spark.sparkContext.setLogLevel("DEBUG")
    table_machines_astra_to_s3(args, spark)
    table_SuidPresentationAudit_astra_to_s3(args, spark)
    table_exploitscohort_astra_to_s3(args, spark)

    table_exploitcohorts_s3_to_astra(spark)

    spark.stop()
  }

  private val licenseSchema = StructType(Array(
    StructField("serialnumber", StringType, nullable = true),
    StructField("dateadded", TimestampType, nullable = true),
    StructField("lastchecked", TimestampType, nullable = true),
    StructField("licensetype", IntegerType, nullable = true),
    StructField("licenseduration", LongType, nullable = true),
    StructField("licenseremaining", LongType, nullable = true),
    StructField("licensestarttime", LongType, nullable = true),
    StructField("licensepguid", StringType, nullable = true),
    StructField("snlocale", StringType, nullable = true),
    StructField("snplatform", StringType, nullable = true),
    StructField("sntype", StringType, nullable = true),
    StructField("licenseexceptiontype", StringType, nullable = true),
    StructField("licensingleid", StringType, nullable = true),
    StructField("productleid", StringType, nullable = true),
    StructField("missingfromlastdata", BooleanType, nullable = true),
    StructField("detectedlicensetype", StringType, nullable = true),
    StructField("hastrialoverride", BooleanType, nullable = true),
    StructField("ownerguid", StringType, nullable = true),
    StructField("pguid_anon", StringType, nullable = true)
  ))

  private val relationshipProfileSchema = StructType(Array(
    StructField("licenseid", StringType, nullable = true),
    StructField("licenseexpirytimestamp", TimestampType, nullable = true),
    StructField("appentitlementstatus", StringType, nullable = true),
    StructField("activationmode", StringType, nullable = true),
    StructField("billingstatus", StringType, nullable = true),
    StructField("activelicense", BooleanType, nullable = true)
  ))

  private val nglProfileSchema = StructType(Array(
    StructField("relationshipprofile", ArrayType(relationshipProfileSchema, containsNull = true), nullable = true),
    StructField("userpguid", StringType, nullable = true),
    StructField("usercountrycode", StringType, nullable = true),
    StructField("deviceid", StringType, nullable = true),
    StructField("osuserid", StringType, nullable = true),
    StructField("nglmode", StringType, nullable = true),
    StructField("creationtime", TimestampType, nullable = true),
    StructField("cachelifetime", LongType, nullable = true),
    StructField("pguid_anon", StringType, nullable = true)
  ))

  private val productHardeningInnerSchema = StructType(Array(
    StructField("statename", StringType, nullable = true),
    StructField("statevalueinfo", MapType(StringType, StringType, valueContainsNull = true), nullable = true)
  ))

  private val productHardeninginfoSchema = StructType(Array(
    StructField("appid", StringType, nullable = true),
    StructField("appexe", StringType, nullable = true),
    StructField("appversion", StringType, nullable = true),
    StructField("msgcreationtimestamp", TimestampType, nullable = true),
    StructField("deviceid", StringType, nullable = true),
    StructField("machineguid", StringType, nullable = true),
    StructField("contextguid", StringType, nullable = true),
    StructField("appstate", ArrayType(productHardeningInnerSchema, containsNull = true), nullable = true)
  ))

  private val filesSchema = StructType(Array(
    StructField("name", StringType, nullable = true),
    StructField("signed", BooleanType, nullable = true),
    StructField("failurereason", StringType, nullable = true),
    StructField("version", StringType, nullable = true),
    StructField("checksum", StringType, nullable = true),
    StructField("signerinfo", StringType, nullable = true)
  ))

  private val leidsSchema = StructType(Array(
    StructField("leid", StringType, nullable = true),
    StructField("locale", StringType, nullable = true),
    StructField("installerid", StringType, nullable = true),
    StructField("files", MapType(StringType, filesSchema, valueContainsNull = true), nullable = true),
    StructField("lastrulefetched", TimestampType, nullable = true),
    StructField("firstrulefetched", TimestampType, nullable = true),
    StructField("licenses", ArrayType(StringType, containsNull = true), nullable = true),
    StructField("invocationagents", MapType(StringType, TimestampType, valueContainsNull = true), nullable = true),
    StructField("installationtime", TimestampType, nullable = true),
    StructField("nglapp", BooleanType, nullable = true),
    StructField("deploymentmode", StringType, nullable = true),
    StructField("nglprofiles", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("nglappid", StringType, nullable = true),
    StructField("nglappversion", StringType, nullable = true),
    StructField("operatingconfigids", ArrayType(StringType, containsNull = true), nullable = true)
  ))

  private val operatingConfigsSchema = StructType(Array(
    StructField("id", StringType, nullable = true),
    StructField("npd_id", StringType, nullable = true),
    StructField("ngl_app_id", StringType, nullable = true),
    StructField("npd_precedence", IntegerType, nullable = true),
    StructField("deployment_mode", StringType, nullable = true),
    StructField("branding_name", StringType, nullable = true)
  ))

  private val machineSchema = StructType(Array(
    StructField("suid", StringType, nullable = false),
    StructField("guid", StringType, nullable = true),
    StructField("machineid", StringType, nullable = true),
    StructField("ngldeviceid", StringType, nullable = true),
    StructField("icclientreferenceid", StringType, nullable = true),
    StructField("os", StringType, nullable = true),
    StructField("osversion", StringType, nullable = true),
    StructField("oslocale", StringType, nullable = true),
    StructField("gcclientversion", StringType, nullable = true),
    StructField("gcupdaterversion", StringType, nullable = true),
    StructField("lastupdatecheck", TimestampType, nullable = true),
    StructField("firstupdatecheck", TimestampType, nullable = true),
    StructField("lastrulefetched", TimestampType, nullable = true),
    StructField("firstrulefetched", TimestampType, nullable = true),
    StructField("lastruleprocessed", TimestampType, nullable = true),
    StructField("firstruleprocessed", TimestampType, nullable = true),
    StructField("firstdispatchfetched", TimestampType, nullable = true),
    StructField("lastdispatchfetched", TimestampType, nullable = true),
    StructField("hostfileentries", ArrayType(StringType, containsNull = true), nullable = true),
    StructField("country", IntegerType, nullable = true),
    StructField("region", IntegerType, nullable = true),
    StructField("city", IntegerType, nullable = true),
    StructField("ip", StringType, nullable = true),
    StructField("domainname", StringType, nullable = true),
    StructField("organizationname", StringType, nullable = true),
    StructField("thorinstalled", BooleanType, nullable = true),
    StructField("licenses", ArrayType(licenseSchema, containsNull = true), nullable = true),
    StructField("nglprofiles", MapType(StringType, nglProfileSchema, valueContainsNull = true), nullable = true),
    StructField("producthardeningappinfo", ArrayType(productHardeninginfoSchema, containsNull = true), nullable = true),
    StructField("leids", ArrayType(leidsSchema, containsNull = true), nullable = true),
    StructField("files", ArrayType(filesSchema, containsNull = true), nullable = true),
    StructField("operatingconfigs", ArrayType(operatingConfigsSchema, containsNull = true), nullable = true),
    StructField("clientuninstalled", BooleanType, nullable = true),
    StructField("clientuninstallreason", StringType, nullable = true),
    StructField("clientuninstalldate", TimestampType, nullable = true),
    StructField("markedforuninstall", BooleanType, nullable = true),
    StructField("clientsource", StringType, nullable = true),
    StructField("lasttimeproductless", TimestampType, nullable = true),
    StructField("uninstallruletype", StringType, nullable = true),
    StructField("invocationagents", MapType(StringType, TimestampType, valueContainsNull = true), nullable = true),
    StructField("allocatedgids", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
    StructField("ipaddresses", ArrayType(StringType, containsNull = true), nullable = true),
    StructField("dnsdomainname", StringType, nullable = true),
    StructField("dnshostname", StringType, nullable = true),
    StructField("workgroup", StringType, nullable = true),
    StructField("agsservicestatus", StringType, nullable = true),
    StructField("updatesuppressed", BooleanType, nullable = true),
    StructField("lastupdaterequeststatus", StringType, nullable = true),
    StructField("lastupdateattempted", TimestampType, nullable = true),
    StructField("systemmanufacturer", StringType, nullable = true),
    StructField("systemmodel", StringType, nullable = true),
    StructField("ipbelongstoisp", BooleanType, true),
    StructField("ispname", StringType, true),
    StructField("homebusinesstype", StringType, true),
    StructField("insert_date", StringType, nullable = true),
    StructField("run_sequence", StringType, nullable = true)
  ))
  private def table_SuidPresentationAudit_astra_to_s3(args: Array[String], spark: SparkSession) = {
    // Specify your bucket
    val bucketPath = args(0)

//    val tableName = "gc_machine_presentation_audit_raw"
//    val dbName = "raw_astra"

    val cassandra_keyspace = "adobe_gc"

//    val maxEventTime = FunctionalHelper.getMaxTimeFromStringType("event_time", "gc_machine_presentation_audit", cassandra_keyspace, spark)
//    println("maxEventTime impl: " + maxEventTime.shortValue())

    val limitTime = FunctionalHelper.getLimitTimestamp()
    println("limitTime impl: " + limitTime.shortValue())

    val runSequence: Int = 1
    val runSequenceStr = runSequence.toString

    val scb = args(0) //"file:///Users/mike.maher/Downloads/secure-connect-pl-test2.zip"
    val host = args(1) //"ccd0908e-fa52-4399-ac61-5306bbede52d-us-west-2.db.astra.datastax.com"
    val clientid = args(2) //"yDrZECwJZJyNedCXuIsSDxSZ"
    val tokenpwd = args(3) //"zTLcSGGoZTBZCGHBt0KIDO3H20P1nEZGE-OZUD900NLiFGAKhw9xywiZescs2qqnr24MuF.RthnOZ_UUbv9D+2BEmWEl6kPxChZOkaW23qkk2gEXAlEsj4ZyxT8OLajO"

    val spa = spark.read.format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.config.cloud.path", scb)
      .option("spark.cassandra.connection.host", host)
      .option("spark.cassandra.auth.username", clientid)
      .option("spark.cassandra.auth.password", tokenpwd)
      .options(Map("table" -> "suid_presentation_audit", "keyspace" -> cassandra_keyspace)).load()

    println("spa count: " + spa.count())

    val SuidPresentationAuditSchema: StructType = StructType(Array(
      StructField("suid", StringType, nullable = true),
      StructField("serial_number", StringType, nullable = true),
      StructField("cohort_id", StringType, nullable = true),
      StructField("country_code", StringType, nullable = true),
      StructField("country_name", StringType, nullable = true),
      StructField("exploit_id", ArrayType(StringType), nullable = true),
      StructField("product_leid", ArrayType(StringType), nullable = true),
      StructField("is_binary_modified", MapType(StringType, BooleanType), nullable = true),
      StructField("non_genuine_leids", ArrayType(StringType), nullable = true),
      StructField("is_hostfile_modified", BooleanType, nullable = true),
      StructField("is_deleted", BooleanType, nullable = true),
      StructField("update_date", TimestampType, nullable = true),
      StructField("external_campaign", StringType, nullable = true),
      StructField("targeting_process", StringType, nullable = true),
      StructField("launch_date", StringType, nullable = true), // Assuming this is a string representation of a date; change to DateType or TimestampType if you have the date in a different format
      StructField("target_category", StringType, nullable = true),
      StructField("target_detarget_reason", StringType, nullable = true),
      StructField("inapp_checkout_enabled", BooleanType, nullable = true),
      StructField("checksum", StringType, nullable = true)
      // Add any other fields that are part of your ExploitCohorts schema.
    ))

    val encoder = RowEncoder(SuidPresentationAuditSchema)
    val currentDate = DateTime.now() //FunctionalHelper.getCurrentDate()
    val res = spa.map {
      row =>
        val suid = row.getAs[String]("suid")
        val rawEventTime = row.getAs[Date]("eventtime")
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        val event_time = dateFormat.format(rawEventTime)
        var event_type = row.getAs[String]("eventtype")
        if (event_type == null) {
          event_type = ""
        }
        var gid = row.getAs[String]("cartid")
        if (gid == null) {
          gid = ""
        }
        var gtoken = row.getAs[String]("gtoken")
        if (gtoken == null) {
          gtoken = ""
        }
        val city = row.getAs[Int]("city")
        val state = row.getAs[Int]("region")
        val country = row.getAs[Int]("country")
        var notifiedleid = row.getAs[String]("activeleid")
        if (notifiedleid == null) {
          notifiedleid = ""
        }
        var notifurl = row.getAs[String]("notifurl")
        if (notifurl == null) {
          notifurl = ""
        }
        var notifauditid = row.getAs[String]("notif_audit_id")
        if (notifauditid == null) {
          notifauditid = ""
        }
        Row(suid, event_time, event_type, gid, gtoken, city, state, country, notifiedleid, notifurl, notifauditid, currentDate, runSequenceStr)
    }(encoder)

    println("res count: " + res.count())
    import java.time.Instant
    val currentTimeMillis: Long = Instant.now().toEpochMilli()


    // This is a very efficient query cause it uses eventTime which is apart of the pk for suid_presentation_audit
    // no table scans generaged via allow filtering cause it used apart of the pk
    val newRows = res.filter(row => {
      val rawEventTime = row.getAs[String](1)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:m:ss.SSS")
      val eventTimeM = format.parse(rawEventTime).getTime
      (currentTimeMillis < eventTimeM) && (eventTimeM < limitTime)
    })

//    val newRows = res.filter(row => {
//      val rawEventTime = row.getAs[String](1)
//      val format = new SimpleDateFormat("yyyy-MM-dd HH:m:ss.SSS")
//      val eventTimeM = format.parse(rawEventTime).getTime
//      (maxEventTime < eventTimeM) && (eventTimeM < limitTime)
//    })

    println("newRows count: " + newRows.count())

//    val destinationPath: String = bucketPath + "gc_machine_presentation_audit_raw"
//    newRows.coalesce(100).write.option("compress", "snappy").mode(SaveMode.Overwrite).partitionBy("insert_date", "run_sequence").parquet(destinationPath)
//    FunctionalHelper.addAndAuditPartition(tableName, currentDate, dbName, runSequence, spark)
  }

  private def table_exploitscohort_astra_to_s3(args: Array[String], spark: SparkSession) = {
    // Specify your bucket
//    val bucketPath = args(0)
    val cassandra_keyspace = "adobe_gc"
//    spark.sql("use `processed`")

    import org.apache.spark.sql.types._

    val cohortDefinitionDisabledSchema: StructType = StructType(Array(
      StructField("suid", StringType, nullable = false),
      StructField("serialnumber", StringType, nullable = true),
      StructField("cohortid", StringType, nullable = true),
      StructField("exploitid", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("leids", ArrayType(StringType, containsNull = true), nullable = true),
      StructField("binarymod", MapType(StringType, BooleanType,
        valueContainsNull = true), nullable = true),
      StructField("hostfilemod", BooleanType, nullable = true),
      StructField("disabled", BooleanType, nullable = true),
      StructField("isdeleted", BooleanType, nullable = true),
      StructField("externalcampaign", StringType, nullable = true),
      StructField("lastupdatetime", TimestampType, nullable = true),
      StructField("insert_date", StringType, nullable = true),
      StructField("run_sequence", StringType, nullable = true)
      // Add any other fields that are part of your ExploitCohorts schema.
    ))

    val scb = args(0)//"file:///Users/mike.maher/Downloads/secure-connect-pl-test2.zip"
    val host = args(1)//"ccd0908e-fa52-4399-ac61-5306bbede52d-us-west-2.db.astra.datastax.com"
    val clientid = args(2)//"yDrZECwJZJyNedCXuIsSDxSZ"
    val tokenpwd = args(3)//"zTLcSGGoZTBZCGHBt0KIDO3H20P1nEZGE-OZUD900NLiFGAKhw9xywiZescs2qqnr24MuF.RthnOZ_UUbv9D+2BEmWEl6kPxChZOkaW23qkk2gEXAlEsj4ZyxT8OLajO"

    val exploitCohorts = spark.read.format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.config.cloud.path", scb)
      .option("spark.cassandra.connection.host", host)
      .option("spark.cassandra.auth.username", clientid)
      .option("spark.cassandra.auth.password", tokenpwd)
      .options(Map("table" -> "exploitcohorts", "keyspace" -> cassandra_keyspace)).load()

    println("exploitCohorts count: " + exploitCohorts.count())

    val exploitCohortsFiltered = exploitCohorts.filter(row =>
      row.getAs[Boolean]("disabled"))

    val encoder = RowEncoder(cohortDefinitionDisabledSchema)
    val currentDate = DateTime.now()
    val runSequence: Int = 1
    val runSequenceStr = runSequence.toString

    println("exploitCohortsFiltered count: " + exploitCohortsFiltered.count())

    val result = exploitCohortsFiltered.map {
      row =>
        val suid = row.getAs[String]("suid")
        val serialnumber = row.getAs[String]("serialnumber")
        val cohortid = row.getAs[String]("cohortid")
        val exploitidSeq = row.getAs[Seq[String]]("exploitid")
        val exploitid = if (exploitidSeq == null) null else exploitidSeq.toList
        val leidsSet = row.getAs[Seq[String]]("leids")
        val leids = if (leidsSet == null) null else leidsSet.toList
        val binarymod = row.getAs[Map[String, Boolean]]("binarymod")
        val hostfilemod = row.getAs[Boolean]("hostfilemod")
        val disabled = row.getAs[Boolean]("disabled")
        val isdeleted = row.getAs[Boolean]("isdeleted")
        val externalcampaign = row.getAs[String]("externalcampaign")
        val lastupdatetime = row.getAs[Date]("lastupdatetime")

        Row(suid, serialnumber, cohortid, exploitid, leids, binarymod, hostfilemod, disabled, isdeleted, externalcampaign, lastupdatetime, currentDate, runSequenceStr)
    }(encoder)

    println("result count: " + result.count())

//    val tableName = "gc_target_disabled_raw"
//    val s3DeltaLocation = "gc_target_disabled_raw_delta"
    //save meta-data on S3
    // FunctionalHelper.saveMetaData(tableName, currentDate, runSequence, "gocart", res.count(), spark)


//    val destinationPath: String = bucketPath + s3DeltaLocation
//    res.coalesce(100)
//      .write
//      .option("compress", "snappy")
//      .format("delta")
//      .partitionBy("insert_date", "run_sequence")
//      .mode(SaveMode.Overwrite)
//      .option("replaceWhere", s"insert_date == '$currentDate' AND run_sequence == '$runSequenceStr'")
//      .save(destinationPath)
//    FunctionalHelper.addAndAuditPartition(tableName, currentDate, runSequence, spark)

  }
  private def table_exploitcohorts_s3_to_astra(spark: SparkSession): Unit = {
    val cassandra_keyspace = "gc"
    spark.sql("use `processed`")

    import org.apache.spark.sql.types._

    val ExploitCohortsSchema: StructType = StructType(Array(
      StructField("suid", StringType, nullable = true),
      StructField("serial_number", StringType, nullable = true),
      StructField("cohort_id", StringType, nullable = true),
      StructField("country_code", StringType, nullable = true),
      StructField("country_name", StringType, nullable = true),
      StructField("exploit_id", ArrayType(StringType), nullable = true),
      StructField("product_leid", ArrayType(StringType), nullable = true),
      StructField("is_binary_modified", MapType(StringType, BooleanType), nullable = true),
      StructField("non_genuine_leids", ArrayType(StringType), nullable = true),
      StructField("is_hostfile_modified", BooleanType, nullable = true),
      StructField("is_deleted", BooleanType, nullable = true),
      StructField("update_date", TimestampType, nullable = true),
      StructField("external_campaign", StringType, nullable = true),
      StructField("targeting_process", StringType, nullable = true),
      StructField("launch_date", StringType, nullable = true), // Assuming this is a string representation of a date; change to DateType or TimestampType if you have the date in a different format
      StructField("target_category", StringType, nullable = true),
      StructField("target_detarget_reason", StringType, nullable = true),
      StructField("inapp_checkout_enabled", BooleanType, nullable = true),
      StructField("checksum", StringType, nullable = true)
      // Add any other fields that are part of your ExploitCohorts schema.
    ))

    val encoder = RowEncoder(ExploitCohortsSchema)

    val cohortDefinitionData = spark.sql("SELECT a.*, md5(concat(coalesce(cast(suid as STRING), ''),coalesce(cast(serial_number as STRING), ''),coalesce(cast(cohort_id as STRING), ''),coalesce(cast(country_code as STRING), '')," +
      "coalesce(cast(country_name as STRING), ''),coalesce(to_json(exploit_id), ''),coalesce(to_json(product_leid), ''),coalesce(to_json(is_binary_modified), ''),coalesce(to_json(non_genuine_leids), ''),coalesce(cast(is_hostfile_modified as STRING), ''),coalesce(cast(is_deleted as STRING), '')," +
      "coalesce(cast(update_date as STRING), ''),coalesce(cast(external_campaign as STRING), ''),coalesce(cast(targeting_process as STRING), ''),coalesce(cast(launch_date as STRING), ''),coalesce(cast(target_category as STRING), ''),coalesce(cast(target_detarget_reason as STRING), ''),,coalesce(cast(inapp_checkout_enabled as STRING), ''))) as checksum " +
      "from gc_cohort_launch_detail as a where is_active_flag=true AND suid NOT IN ('013944be-4d4c-48f8-9630-b23b995deb5154', '013944be-4d4c-48f8-9630-b23b995deb5146','013944be-4d4c-48f8-9630-b23b995deb5151','013944be-4d4c-48f8-9630-b23b995deb5145', 'UNATTRIBUTED')")

    val cohortDefinition = cohortDefinitionData.map(row => {
      val suid = row.getAs[String]("suid")
      val serialNumber = Option(row.getAs[String]("serial_number")).orNull
      val cohortId = Option(row.getAs[String]("cohort_id")).orNull
      val exploitId = Option(row.getAs[Seq[String]]("exploit_id")).orNull
      val leids = Option(row.getAs[Seq[String]]("product_leid")).orNull
      val isBinaryModified = Option(row.getAs[Map[String, Boolean]]("is_binary_modified")).orNull
      val nonGenuineLeids = Option(row.getAs[Seq[String]]("non_genuine_leids")).orNull
      val isHostFileModified = Option(row.getAs[Boolean]("is_hostfile_modified")).getOrElse(false)
      val isDeleted = Option(row.getAs[Boolean]("is_deleted")).getOrElse(false)
      //      val lastUpdateTime = Option(row.getAs[Timestamp]("update_date")).getOrElse(Timestamp.from(Instant.now()))
      val externalCampaign = Option(row.getAs[String]("external_campaign")).orNull
      val targetingProcess = Option(row.getAs[String]("targeting_process")).orNull
      val launchDate = Option(row.getAs[String]("launch_date")).orNull
      val targetFamily = Option(row.getAs[String]("target_category")).orNull
      val targetDeTargetReason = Option(row.getAs[String]("target_detarget_reason")).orNull
      val inAppCheckoutEnabled = Option(row.getAs[Boolean]("inapp_checkout_enabled")).getOrElse(false)
      val checksum = Option(row.getAs[String]("checksum")).orNull
      //      Row(suid, isBinaryModified, checksum, cohortId, false, exploitId, externalCampaign,
      //        isHostFileModified, isDeleted, lastUpdateTime, launchDate, leids,
      //        nonGenuineLeids, serialNumber, false, targetDeTargetReason, targetFamily, false, targetingProcess, inAppCheckoutEnabled)

      Row(suid, isBinaryModified, checksum, cohortId, false, exploitId, externalCampaign,
        isHostFileModified, isDeleted, launchDate, leids,
        nonGenuineLeids, serialNumber, false, targetDeTargetReason, targetFamily, false, targetingProcess, inAppCheckoutEnabled)
    })(encoder)

//    cohortDefinitionData.explain(true)

    val columnsToCompare = Seq("suid", "checksum")
    val exploitCohortsCass = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "exploitcohorts", "keyspace" -> cassandra_keyspace)).load()

    val rowToInsert = cohortDefinition.join(exploitCohortsCass, columnsToCompare, "leftouter")
      .filter(cohortDefinition("checksum").notEqual(exploitCohortsCass("checksum")).or(exploitCohortsCass("checksum").isNull))
      .select(Seq(cohortDefinition("suid"),
        cohortDefinition("binarymod"),
        cohortDefinition("checksum"),
        cohortDefinition("cohortid"),
        cohortDefinition("disabled"),
        cohortDefinition("exploitid"),
        cohortDefinition("externalcampaign"),
        cohortDefinition("hostfilemod"),
        cohortDefinition("isdeleted"),
        cohortDefinition("lastupdatetime"),
        cohortDefinition("launchdate"),
        cohortDefinition("leids"),
        cohortDefinition("non_genuine_leids"),
        cohortDefinition("serialnumber"),
        cohortDefinition("syncedtosophia"),
        cohortDefinition("target_detarget_reason"),
        cohortDefinition("target_family"),
        cohortDefinition("target_for_patching"),
        cohortDefinition("targeting_process"),
        cohortDefinition("inapp_checkout_enabled")): _*)

    rowToInsert.write.format("org.apache.spark.sql.cassandra").mode("append")
      .option("table", "exploitcohorts")
      .option("keyspace", cassandra_keyspace).save()
  }

  private def table_machines_astra_to_s3(args: Array[String], spark: SparkSession) = {
    // Specify your bucket
    //    val bucketPath = args(0)
    val cassandra_keyspace = "adobe_gc"
    //    spark.sql("use `processed`")

    import org.apache.spark.sql.types._

//    val queryString: String = "SELECT greatest(max(lastrulefetched), max(lastruleprocessed), max(lastdispatchfetched), " + "max(lastupdatecheck)) as max_event_time from gc_machine"

//    val maxEventTime = FunctionalHelper.getAndFormatMaxTime(queryString, "gc_machine", "curated", spark)
    val limitTime = FunctionalHelper.getLimitTimestamp()
    val runSequence: Int = 1
    val runSequenceStr = runSequence.toString

    val scb = args(0) //"file:///Users/mike.maher/Downloads/secure-connect-pl-test2.zip"
    val host = args(1) //"ccd0908e-fa52-4399-ac61-5306bbede52d-us-west-2.db.astra.datastax.com"
    val clientid = args(2) //"yDrZECwJZJyNedCXuIsSDxSZ"
    val tokenpwd = args(3) //"zTLcSGGoZTBZCGHBt0KIDO3H20P1nEZGE-OZUD900NLiFGAKhw9xywiZescs2qqnr24MuF.RthnOZ_UUbv9D+2BEmWEl6kPxChZOkaW23qkk2gEXAlEsj4ZyxT8OLajO"

    val machines = spark.read.format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.config.cloud.path", scb)
      .option("spark.cassandra.connection.host", host)
      .option("spark.cassandra.auth.username", clientid)
      .option("spark.cassandra.auth.password", tokenpwd)
      .options(Map("table" -> "machines", "keyspace" -> cassandra_keyspace)).load()

    println("machines count: " + machines.count())

//    val leidsSchema: StructType = StructType(Array(
//      StructField("leid", StringType, nullable = true),
//      StructField("locale", StringType, nullable = true),
//      StructField("installerid", StringType, nullable = true),
//      StructField("files", ArrayType(StringType, containsNull = true), nullable = true),
//      StructField("lastrulefetched", TimestampType, nullable = true),
//      StructField("firstrulefetched", TimestampType, nullable = true),
//      StructField("licenses", ArrayType(StringType, containsNull = true), nullable = true),
//      StructField("invocationagents", ArrayType(StringType, containsNull = true), nullable = true),
//      StructField("installationtime", TimestampType, nullable = true),
//      StructField("nglapp", BooleanType, nullable = true),
//      StructField("deploymentmode", StringType, nullable = true),
//      StructField("nglprofiles", ArrayType(StringType, containsNull = true), nullable = true),
//      StructField("nglappid", StringType, nullable = true),
//      StructField("nglappversion", StringType, nullable = true),
//      StructField("operatingconfigids", ArrayType(StringType, containsNull = true), nullable = true)
//    ))


//    val machineSchema: StructType = StructType(Array(
//      // Example fields - adjust according to your actual schema
//      StructField("suid", StringType, nullable = true),
//      StructField("machine_name", StringType, nullable = true),
//      StructField("os_version", StringType, nullable = true),
//      // The transformed leids field, with its schema being a Map of String to the leidsSchema defined above
//      StructField("leids", MapType(StringType, leidsSchema), nullable = true),
//      // Include other fields as per your table structure
//    ))

    val encoder = RowEncoder(machineSchema)
    val currentDate = DateTime.now()

    val machinesMapped = machines.withColumn("leids",
      functions.expr(
        "transform_values(leids, (key, value) -> struct(value.leid, value.locale, value.installerid, value.files, " +
          "value.lastrulefetched, value.firstrulefetched, value.licenses, value.invocationagents, " +
          "value.installationtime,  coalesce(value.nglapp, false), value.deploymentmode, value" +
          ".nglprofiles, value.nglappid, value.nglappversion, value.operatingconfigids))"
      ).cast(MapType(StringType, leidsSchema))
    )

    println("machinesMapped count: " + machinesMapped.count())

    val res = machinesMapped.map {
      row =>
        val suid = row.getAs[String]("suid")
        val guid = row.getAs[String]("guid")
        val machineid = row.getAs[String]("machineid")
        val ngldeviceid = row.getAs[String]("ngldeviceid")
        val icclientreferenceid = row.getAs[String]("icclientreferenceid")
        val os = row.getAs[String]("os")
        val osversion = row.getAs[String]("osversion")
        val oslocale = row.getAs[String]("oslocale")
        val gcclientversion = row.getAs[String]("gcclientversion")
        val gcupdaterversion = row.getAs[String]("gcupdaterversion")
        val lastupdatecheck = row.getAs[Date]("lastupdatecheck")
        val firstupdatecheck = row.getAs[Date]("firstupdatecheck")
        val lastrulefetched = row.getAs[Date]("lastrulefetched")
        val firstrulefetched = row.getAs[Date]("firstrulefetched")
        val lastruleprocessed = row.getAs[Date]("lastruleprocessed")
        val firstruleprocessed = row.getAs[Date]("firstruleprocessed")
        val firstdispatchfetched = row.getAs[Date]("firstdispatchfetched")
        val lastdispatchfetched = row.getAs[Date]("lastdispatchfetched")
        val hostfileentries = row.getAs[Seq[String]]("hostfileentries")
        val country = row.getAs[Int]("country")
        val region = row.getAs[Int]("region")
        val city = row.getAs[Int]("city")
        val ip = row.getAs[String]("ip")
        val domainname = row.getAs[String]("domainname")
        val organizationname = row.getAs[String]("organizationname")
        val thorInstalled = row.getAs[Boolean]("thorinstalled")
        val licensesMap = row.getAs[Map[String, com.datastax.spark.connector.UDTValue]]("licenses")
        val licenses = if (licensesMap == null) null else licensesMap.values.toList
        val nglprofiles = row.getAs[Map[String, com.datastax.spark.connector.UDTValue]]("nglprofiles")
        val producthardeningappinfoMap = row.getAs[Map[String, com.datastax.spark.connector.UDTValue]]("producthardeningappinfo")
        val producthardeningappinfo = if (producthardeningappinfoMap == null) null else producthardeningappinfoMap.values.toList
        val leidsMap = row.getAs[Map[String, org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema]]("leids")
        val leids = if (leidsMap == null) null else leidsMap.values.toList.filter(x => {
          val fileDefined = x.getAs[Map[String, com.datastax.spark.connector.UDTValue]]("files").nonEmpty
          val licenseDefined = x.getAs[List[String]]("licenses").nonEmpty
          fileDefined || licenseDefined
        })

        val filesMap = row.getAs[Map[String, com.datastax.spark.connector.UDTValue]]("files")
        val files = if (filesMap == null) null else filesMap.values.toList
        val operatingConfigsMap = row.getAs[Map[String, com.datastax.spark.connector.UDTValue]]("operatingconfigs")
        val operatingconfigs = if (operatingConfigsMap == null) null else operatingConfigsMap.values.toList
        val clientuninstalled = row.getAs[Boolean]("clientuninstalled")
        val clientuninstallreason = row.getAs[String]("clientuninstallreason")
        val clientuninstalldate = row.getAs[Date]("clientuninstalldate")
        val markedforuninstall = row.getAs[Boolean]("markedforuninstall")
        val clientsource = row.getAs[String]("clientsource")
        val lasttimeproductless = row.getAs[Date]("lasttimeproductless")
        val uninstallruletype = row.getAs[String]("uninstallruletype")
        val invocationagents = row.getAs[Map[String, Date]]("invocationagents")
        val allocatedgids = row.getAs[Map[String, String]]("cartids")
        val ipaddressesSeq = row.getAs[Seq[String]]("ipaddresses")
        val ipaddresses = if (ipaddressesSeq == null) null else ipaddressesSeq.toList
        val dnsdomainname = row.getAs[String]("dnsdomainname")
        val dnshostname = row.getAs[String]("dnshostname")
        val workgroup = row.getAs[String]("workgroup")
        val clientupdateinfo = row.getAs[Row]("clientupdateinfo")
        val agsservicestatus = if (clientupdateinfo == null) null else clientupdateinfo.getAs[String]("agsservicestatus")
        val updatesuppressed = if (clientupdateinfo == null) false else clientupdateinfo.getAs[Boolean]("updatesuppressed")
        val lastupdaterequeststatus = if (clientupdateinfo == null) null else clientupdateinfo.getAs[String]("lastupdaterequeststatus")
        val lastupdateattempted = if (clientupdateinfo == null) null else clientupdateinfo.getAs[Date]("lastupdateattempted")
        val systemmanufacturer = row.getAs[String]("systemmanufacturer")
        val systemmodel = row.getAs[String]("systemmodel")
        val ipBelongsToIsp = row.getAs[Boolean]("ipbelongstoisp")
        val ispName = row.getAs[String]("ispname")
        val homeBusinessType = row.getAs[String]("homebusinesstype")

        Row(suid, guid, machineid, ngldeviceid, icclientreferenceid, os, osversion, oslocale,
          gcclientversion, gcupdaterversion, lastupdatecheck, firstupdatecheck,
          lastrulefetched, firstrulefetched, lastruleprocessed, firstruleprocessed,
          firstdispatchfetched, lastdispatchfetched, hostfileentries, country, region,
          city, ip, domainname, organizationname, thorInstalled, licenses, nglprofiles,
          producthardeningappinfo, leids, files, operatingconfigs,
          clientuninstalled, clientuninstallreason, clientuninstalldate, markedforuninstall,
          clientsource, lasttimeproductless, uninstallruletype, invocationagents,
          allocatedgids, ipaddresses, dnsdomainname, dnshostname, workgroup,
          agsservicestatus, updatesuppressed, lastupdaterequeststatus, lastupdateattempted,
          systemmanufacturer, systemmodel, ipBelongsToIsp, ispName, homeBusinessType, currentDate, runSequenceStr
        )
    }(encoder)

    println("res count: " + res.count())
    import java.time.Instant
    val currentTimeMillis: Long = Instant.now().toEpochMilli()

    val newRows = res.filter(row => {
      val lastRuleFetched = Option(row.getAs[Date](12)).getOrElse(new Date(0))
      val lastRuleProcessed = Option(row.getAs[Date](14)).getOrElse(new Date(0))
      val lastUpdateCheck = Option(row.getAs[Date](10)).getOrElse(new Date(0))
      val lastDispatchFetch = Option(row.getAs[Date](17)).getOrElse(new Date(0))
      val maxLastDate = Seq(lastRuleFetched, lastRuleProcessed, lastUpdateCheck, lastDispatchFetch).max.getTime
      (currentTimeMillis < maxLastDate) && (maxLastDate < limitTime)
    }).filter(row => Option(row.getAs[String](2)).isDefined)

    println("newRows count: " + newRows.count())

  }
}


