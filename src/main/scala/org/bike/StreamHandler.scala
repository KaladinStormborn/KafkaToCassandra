package org.bike

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StructType}

object StreamHandler extends App {

  // initialize Spark
  val spark = SparkSession
    .builder
    .appName("Stream Handler")
    .master("local[*]")
    .config("spark.cassandra.connection.host", "localhost")
    .getOrCreate()

  // read from Kafka
  val inputDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "trip-events-topic")
    .load()

  // only select 'value' from the table,
  // convert from bytes to string
  val rawDF = inputDF.selectExpr("CAST(value AS STRING)")

  val stringType = DataTypes.StringType
  val timestampType = DataTypes.TimestampType
  val doubleType = DataTypes.DoubleType

  // scheme for parse the csv message
  val schema = new StructType()
    .add("ride_id", stringType)
    .add("rideable_type", stringType)
    .add("started_time", timestampType)
    .add("ended_time", timestampType)
    .add("start_station", stringType)
    .add("start_station_id", stringType)
    .add("end_station", stringType)
    .add("end_station_id", stringType)
    .add("start_lat", doubleType)
    .add("start_ing", doubleType)
    .add("end_lat", doubleType)
    .add("end_ing", doubleType)
    .add("member_casual", stringType)

  val option = Map("delimiter" -> ",")

  // convert messages to a table with values
  val expandedDF = rawDF
    .select(from_csv(col("value"), schema, option).as("trip"))

  // select only useful column
  val outputDF = expandedDF
    .select("trip.ride_id", "trip.started_time", "trip.ended_time", "trip.start_lat", "trip.start_ing", "trip.end_lat", "trip.end_ing", "trip.member_casual")

  // write to cassandra
  val query = outputDF
    .writeStream
    .outputMode(OutputMode.Append)
    .format("org.apache.spark.sql.cassandra")
    .option("checkpointLocation", "checkpoint")
    .option("keyspace", "bike")
    .option("table", "trip")
    .start()

  // until ^C
  query.awaitTermination()
}