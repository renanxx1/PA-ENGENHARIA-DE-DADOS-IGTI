from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == 'main':

    spark = ( SparkSession.builder.appName("spark_processing_api")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate() )

    spark.sparkContext.setLogLevel("WARN")

    bootstrap_server = '10.111.243.160:9092'

    schema = "active_id INT, size INT, at STRING, from STRING, to STRING, id INT, open FLOAT, close FLOAT, min FLOAT, max FLOAT, ask FLOAT, bid FLOAT, volume FLOAT, phase STRING"

    bootstrap_server = '10.106.13.32:9092'

    schema = "active_id INT, size INT, at STRING, from STRING, to STRING, id INT, open FLOAT, close FLOAT, min FLOAT, max FLOAT, ask FLOAT, bid FLOAT, volume FLOAT, phase STRING"

    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribePattern", "ingestion_src_api.*")
        .option("startingOffsets", "latest")
        .load()
        )

    df_processed = (
        df_raw.select(col("topic").cast("string"), from_json(col("value").cast("string"), schema).alias("value")) 
        .withColumn("topic", regexp_replace("topic", "ingestion", "processed"))
        .select("topic", to_json(struct(expr("value.active_id as active_id"), expr("value.size as timeframe"),
        expr("cast(value.at / 1000000000 as timestamp) as executed_at"), expr("FROM_UNIXTIME(value.from) as candle_from"), 
        expr("FROM_UNIXTIME(value.to) as candle_to"), expr("value.id as period"), "value.open", "value.close", "value.min", "value.max", "value.ask", "value.bid", "value.volume"))
        .alias("value")).writeStream.format("kafka").option("kafka.bootstrap.servers", bootstrap_server)
        .option("checkpointLocation", f"./checkpoint/").start()
    )
