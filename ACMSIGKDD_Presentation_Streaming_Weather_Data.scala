// Databricks notebook source
// MAGIC %md 
// MAGIC ###### References
// MAGIC ###### https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
// MAGIC ###### https://databricks.com/blog/2016/11/16/oil-gas-asset-optimization-aws-kinesis-rds-databricks.html

// COMMAND ----------

// MAGIC %md
// MAGIC ### ACM SIG KDD Demo: Spark Streaming using Weather data
// MAGIC ##### Presented on 23-Aug-2017
// MAGIC ##### Ulind Narang

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Steps:  
// MAGIC ###### Create an AWS S3 bucket. 
// MAGIC ###### Load NOAA weather csv file into S3 bucket (use AWS console or AWS CLI) 
// MAGIC ###### Run seperate Python code to convert each row in csv file to a json file, generating multiple json files.
// MAGIC ###### Code in this file assumes that individual json files are availabe in S3 and accessible.  

// COMMAND ----------

// MAGIC %md
// MAGIC #### If S3 bucket is not mounted, then do so now

// COMMAND ----------

// MAGIC %python
// MAGIC # If you have already mounted your S3 bucket, then this code will give an exception and a message that the bucket has already been mounted.
// MAGIC 
// MAGIC # Get ACCESS_KEY and SECRET_KEY from IAM in AWS. Insert the keys into the quotes below.
// MAGIC # Create an S3 bucket and put the name (S3 bucket names are globally unique - so, just the bucket-name)
// MAGIC ACCESS_KEY = "YOUR_IAM_ACCESS_KEY"  
// MAGIC SECRET_KEY = "YOUR_IAM_SECRET_KEY"  
// MAGIC AWS_BUCKET_NAME = "YOUR_S3_BUCKET_NAME"
// MAGIC MOUNT_NAME = "YOUR_S3_BUCKET_LOCAL_MOUNTED_NAME"
// MAGIC 
// MAGIC 
// MAGIC dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
// MAGIC display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))
// MAGIC 
// MAGIC # You can add try and except to catch any errors while mounting the bucket
// MAGIC # Login to S3 console or AWS CLI for accessing the 

// COMMAND ----------

// MAGIC %fs ls /mnt/$MOUNT_NAME/json-files

// COMMAND ----------

// Check a sample json file
%fs head /mnt/$MOUNT_NAME/json-files/WBAN_13904_100001.json

// COMMAND ----------

// MAGIC %md
// MAGIC ### Single file processing with data from one csv file uploaded to S3 

// COMMAND ----------

 
val df_TravisWeather = sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/mnt/$MOUNT_NAME/travis_rawData.csv")  // $MOUNT_NAME is local name of mounted S3 bucket


// COMMAND ----------

display(df_TravisWeather.select("*"))

// COMMAND ----------

df_TravisWeather.printSchema
df_TravisWeather.createOrReplaceTempView("tbl_TravisWeather")

// COMMAND ----------

df_TravisWeather.createOrReplaceTempView("tbl_TravisWeather")


// COMMAND ----------

// import some SQL aggregate and windowing function
import org.apache.spark.sql.functions._
val df_TravisWeatherBrief = sqlContext.sql("select date as datetime, hourlydrybulbtempf as temp, hourlyrelativehumidity as humidity, hourlywindspeed as windspeed from tbl_TravisWeather")
display(df_TravisWeatherBrief.select("*"))


// COMMAND ----------

val df_TempHumidity = df_TravisWeatherBrief
    .select($"temp", $"humidity", $"windspeed")

// COMMAND ----------

df_TempHumidity.createOrReplaceTempView("tbl_TempHumidityWind")

// COMMAND ----------

display(sqlContext.table("tbl_TempHumidityWind").describe())

// COMMAND ----------

val df_TestJson = sqlContext.read.format("json")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/mnt/$MOUNT_NAME/json-files/WBAN_13904_100001.json") // $MOUNT_NAME is local name of mounted S3 bucket

df_TestJson.show()


// COMMAND ----------

df_TestJson.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### Batch processing of json files stored in the S3 folder

// COMMAND ----------


import org.apache.spark.sql.types._

val inputFilesPath = "/mnt/$MOUNT_NAME/json-files/"

//{'HOURLYSeaLevelPressure': ['29.96'], 'HOURLYDRYBULBTEMPF': ['75'], 'STATION': ['WBAN:13904'], 'HOURLYPrecip': [''], 'DATE': ['2007-07-01 00:53'], 'HOURLYWindSpeed': ['6'], 'HOURLYStationPressure': ['29.46s'],  'HOURLYRelativeHumidity': ['90']}

// NOTE: SPARK DOES NOT PROCESS PRETTY PRINTED JSON

val jsonWeatherSchema = new StructType()
  .add("STATION", StringType)
  .add("UNIX_TIMESTAMP", DoubleType)
  .add("HOURLYDRYBULBTEMPF", DoubleType)
  .add("TEMPRANGE", StringType)
  .add("HOURLYRelativeHumidity", DoubleType)
  .add("HOURLYSeaLevelPressure", DoubleType)
  .add("HOURLYStationPressure",DoubleType)
  .add("HOURLYWindSpeed", DoubleType)
  .add("HOURLYPrecip", DoubleType)

val df_staticWeatherInput = 
  spark
    .read
    .schema(jsonWeatherSchema)
    .json(inputFilesPath)

display(df_staticWeatherInput)
df_staticWeatherInput.createOrReplaceTempView("tbl_StaticWeatherInput")



// COMMAND ----------

import org.apache.spark.sql.functions._


val df_TimeTempRange = sqlContext.sql("select from_unixtime(UNIX_TIMESTAMP,'YYYY-MM-dd HH:mm') as `Timestamp`,  HOURLYDRYBULBTEMPF as Temperature, TEMPRANGE as RangeOfTemp from tbl_StaticWeatherInput")

display(df_TimeTempRange)



// COMMAND ----------

df_TimeTemp.createOrReplaceTempView("tbl_TimeTemp")

// COMMAND ----------

val df_TimeTempRangeCounts = 
  df_TimeTempRange
    .groupBy($"RangeOfTemp", window($"TimeStamp", "1 hour"))
    .count()

df_TimeTempRangeCounts.createOrReplaceTempView("tbl_TimeTempRangeCounts")

// COMMAND ----------

// MAGIC %sql select RangeOfTemp, sum(count) as total_count from tbl_TimeTempRangeCounts group by RangeOfTemp

// COMMAND ----------

// MAGIC %sql select RangeOfTemp, date_format(window.end, "MMM-dd HH:mm") as time, count from tbl_TimeTempRangeCounts order by time, RangeOfTemp

// COMMAND ----------

// MAGIC %md
// MAGIC # Streaming of json files and processing the stream

// COMMAND ----------

import org.apache.spark.sql.functions._


val df_StreamWeatherInput = 
  spark
   .readStream
   .schema(jsonWeatherSchema)
   .option("maxFilesPerTrigger", 1)
   .json(inputFilesPath)

df_StreamWeatherInput.createOrReplaceTempView("tbl_StreamWeatherInput")

val df_StreamTimeTempRange = sqlContext.sql("select from_unixtime(UNIX_TIMESTAMP,'YYYY-MM-dd HH:mm') as `Timestamp`,  HOURLYDRYBULBTEMPF as Temperature, TEMPRANGE as RangeOfTemp from tbl_StreamWeatherInput")

display(df_StreamTimeTempRange)


val df_WeatherStreamCounts = 
  df_StreamTimeTempRange
    .groupBy($"RangeOfTemp", window($"Timestamp", "1 hour"))
    .count()


// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")  // keep the size of shuffles small

val query =
  df_WeatherStreamCounts
    .writeStream
    .format("memory")         
    .queryName("counts")      
    .outputMode("complete") 
    .start()

	

// COMMAND ----------

Thread.sleep(5000) // wait a bit for computation to start

// COMMAND ----------

// MAGIC %sql select RangeOfTemp, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, RangeOfTemp

// COMMAND ----------

Thread.sleep(5000)  // wait a bit more for more data to be computed

// COMMAND ----------

// MAGIC %sql select RangeOfTemp, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, RangeOfTemp

// COMMAND ----------

 Thread.sleep(5000)  // wait a bit more for more data to be computed

// COMMAND ----------

// MAGIC %sql select RangeOfTemp, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, RangeOfTemp

// COMMAND ----------

// MAGIC %sql select RangeOfTemp, sum(count) as total_count from counts group by RangeOfTemp order by RangeOfTemp

// COMMAND ----------

// MAGIC %md
// MAGIC #### Demo ends
