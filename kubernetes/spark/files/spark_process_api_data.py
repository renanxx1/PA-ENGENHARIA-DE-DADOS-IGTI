from pyspark.sql import SparkSession
from pyspark.sql.functions import *


if __name__ == 'main':

    bootstrap_server = '10.99.5.38:9092'
    
    spark = ( 
        SparkSession.builder.appName("spark_processing_api")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
        .getOrCreate() 
        )

    spark.sparkContext.setLogLevel("WARN")
    
    spark.range(100).show()
    # topics = [
    # 'ingestion_src_api_iq_BTCUSD_1_json', 'ingestion_src_api_iq_BTCUSD_5_json', 'ingestion_src_api_iq_BTCUSD_60_json', 'ingestion_src_api_iq_BTCUSD_300_json', 'ingestion_src_api_iq_BTCUSD_900_json', 'ingestion_src_api_iq_BTCUSD_1800_json', 'ingestion_src_api_iq_BTCUSD_3600_json', 'ingestion_src_api_iq_BTCUSD_14400_json', 'ingestion_src_api_iq_BTCUSD_86400_json',
    # 'ingestion_src_api_iq_XRPUSD_1_json', 'ingestion_src_api_iq_XRPUSD_5_json', 'ingestion_src_api_iq_XRPUSD_60_json', 'ingestion_src_api_iq_XRPUSD_300_json', 'ingestion_src_api_iq_XRPUSD_900_json', 'ingestion_src_api_iq_XRPUSD_1800_json', 'ingestion_src_api_iq_XRPUSD_3600_json', 'ingestion_src_api_iq_XRPUSD_14400_json', 'ingestion_src_api_iq_XRPUSD_86400_json',
    # 'ingestion_src_api_iq_ETHUSD_1_json', 'ingestion_src_api_iq_ETHUSD_5_json', 'ingestion_src_api_iq_ETHUSD_60_json', 'ingestion_src_api_iq_ETHUSD_300_json', 'ingestion_src_api_iq_ETHUSD_900_json', 'ingestion_src_api_iq_ETHUSD_1800_json', 'ingestion_src_api_iq_ETHUSD_3600_json', 'ingestion_src_api_iq_ETHUSD_14400_json', 'ingestion_src_api_iq_ETHUSD_86400_json',
    # 'ingestion_src_api_iq_OMGUSD_1_json', 'ingestion_src_api_iq_OMGUSD_5_json', 'ingestion_src_api_iq_OMGUSD_60_json', 'ingestion_src_api_iq_OMGUSD_300_json', 'ingestion_src_api_iq_OMGUSD_900_json', 'ingestion_src_api_iq_OMGUSD_1800_json', 'ingestion_src_api_iq_OMGUSD_3600_json', 'ingestion_src_api_iq_OMGUSD_14400_json', 'ingestion_src_api_iq_OMGUSD_86400_json',
    # 'ingestion_src_api_iq_TRXUSD_1_json', 'ingestion_src_api_iq_TRXUSD_5_json', 'ingestion_src_api_iq_TRXUSD_60_json', 'ingestion_src_api_iq_TRXUSD_300_json', 'ingestion_src_api_iq_TRXUSD_900_json', 'ingestion_src_api_iq_TRXUSD_1800_json', 'ingestion_src_api_iq_TRXUSD_3600_json', 'ingestion_src_api_iq_TRXUSD_14400_json', 'ingestion_src_api_iq_TRXUSD_86400_json',
    # ]

    # schema = "active_id INT, size INT, at STRING, from STRING, to STRING, id INT, open FLOAT, close FLOAT, min FLOAT, max FLOAT, ask FLOAT, bid FLOAT, volume FLOAT, phase STRING"

    # for topic in topics:
    #     df = (spark.readStream.format("kafka")
    #                         .option("kafka.bootstrap.servers", bootstrap_server)
    #                         .option("subscribe", topic)
    #                         .option("startingOffsets", "latest")
    #                         .load()
    #                         .select(from_json(col("value").cast("string"), schema).alias("value")) 
    #                         )

    #     df = (df.select(to_json(struct(expr("value.active_id as active_id"), expr("value.size as timeframe"),
    #                         expr("cast(value.at / 1000000000 as timestamp) as executed_at"), expr("FROM_UNIXTIME(value.from) as candle_from"), 
    #                         expr("FROM_UNIXTIME(value.to) as candle_to"), expr("value.id as period"), 
    #                         "value.open", "value.close", "value.min", "value.max", "value.ask", "value.bid", "value.volume")).alias("value"))
    #                         .writeStream.format("kafka").option("kafka.bootstrap.servers", bootstrap_server)
    #                         .option("topic", topic.replace('ingestion', 'processed'))
    #                         .option("checkpointLocation", F"./checkpoint_{topic.replace('ingestion', 'processed')}/")
    #                         .start()   
    #                         )
        