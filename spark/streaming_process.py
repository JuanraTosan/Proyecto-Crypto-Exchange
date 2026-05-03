# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Esquema de los datos de Binance
schema = StructType([
    StructField("s", StringType(), True), 
    StructField("c", StringType(), True), 
    StructField("v", StringType(), True), 
    StructField("E", StringType(), True)  
])

spark = SparkSession.builder \
    .appName("CryptoStreamingProcess") \
    .getOrCreate()

# Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "crypto_raw") \
    .load()

# Transformar JSON y añadir columnas de particionado
processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("price", col("c").cast(DoubleType())) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("fecha_proc", to_date(col("timestamp"))) \
    .withColumnRenamed("s", "par")

# Escribir en HDFS en formato Parquet
query = processed_df.writeStream \
    .partitionBy("par", "fecha_proc") \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data/cripto/") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/crypto/") \
    .outputMode("append") \
    .start()

query.awaitTermination()