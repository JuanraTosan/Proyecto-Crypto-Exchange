# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from prometheus_client import start_http_server, Gauge, Counter

# --- CAPA 2: DEFINICIÓN DE MÉTRICAS DE NEGOCIO ---
# Iniciamos el servidor en el puerto 8000 para que Prometheus lo lea
start_http_server(8000)

# Definimos las métricas solicitadas
PRECIO_ACTUAL = Gauge('crypto_precio_actual', 'Precio actual del par', ['par'])
MENSAJES_TOTAL = Counter('pipeline_mensajes_total', 'Total mensajes procesados', ['par'])

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

# Procesamiento básico
processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("price", col("c").cast(DoubleType())) \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("fecha_proc", to_date(col("timestamp"))) \
    .withColumnRenamed("s", "par")

# --- FUNCIÓN PARA ACTUALIZAR PROMETHEUS POR CADA BATCH ---
def update_prometheus(batch_df, batch_id):
    # 1. Guardar en HDFS (Parquet) como hacías antes
    batch_df.write.partitionBy("par", "fecha_proc") \
        .format("parquet") \
        .mode("append") \
        .save("hdfs://namenode:9000/data/cripto/")
    
    # 2. Actualizar métricas de negocio
    # Obtenemos el último precio de cada par en este micro-batch
    rows = batch_df.select("par", "price").collect()
    for row in rows:
        if row['par'] and row['price']:
            PRECIO_ACTUAL.labels(par=row['par']).set(row['price'])
            MENSAJES_TOTAL.labels(par=row['par']).inc()

# Aplicar la función de escritura y métricas
query = processed_df.writeStream \
    .foreachBatch(update_prometheus) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/crypto/") \
    .start()

query.awaitTermination()