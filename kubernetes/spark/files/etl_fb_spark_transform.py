from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    spark = ( SparkSession.builder.appName("spark_processing_api")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
    .getOrCreate() )

    spark.sparkContext.setLogLevel("WARN")

    spark.range(100).show()
    print("*****************")
    print("Escrito com sucesso!")
    print("*****************")

    spark.stop()
