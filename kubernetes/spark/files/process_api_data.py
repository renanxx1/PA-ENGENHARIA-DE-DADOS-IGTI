from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#import schema

if __name__ == 'main':
    
    spark = ( SparkSession.builder.appName("spark_processing_api")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .config("spark.sql.warehouse.dir", "./warehouse/")
    .enableHiveSupport().getOrCreate() )
    
    spark.sparkContext.setLogLevel("WARN")
    json_options = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"}
    
    df = (spark.readStream.format("kafka")
                       .option("kafka.bootstrap.servers", "10.110.105.242:9092")
                       .option("subscribe", "ingest_src_api_iq_BTCUSD_1_json")
                       .option("startingOffsets", "earliest")
                       .option("endingOffsets", "latest")
                       .option("checkpoint", "checkpoint")
                       .load()
                       #.select(from_json(col("value").cast("sintrg"), schema, json_options).alias("stream"))    
                       )
    df