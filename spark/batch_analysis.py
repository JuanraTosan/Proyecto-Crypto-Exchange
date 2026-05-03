# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, count

spark = SparkSession.builder \
    .appName("CryptoBatchAnalysis") \
    .getOrCreate()

# 1. Leer los datos históricos desde HDFS
print("--- Leyendo datos desde HDFS ---")
df = spark.read.parquet("hdfs://namenode:9000/data/cripto/")

# 2. Análisis estadístico por par (Fase 2.1)
# Calculamos promedio, max, min y cantidad de registros
stats = df.groupBy("par").agg(
    avg("price").alias("precio_promedio"),
    max("price").alias("precio_maximo"),
    min("price").alias("precio_minimo"),
    count("price").alias("total_muestras")
)

stats.show()

# Guardar el resultado en un solo archivo CSV para leerlo fácil
stats.coalesce(1).write.mode("overwrite").option("header", "true").csv("/tmp/resultado_analisis")

# 3. Guardar resultados en una nueva ruta de HDFS
print("--- Guardando resultados del análisis ---")
stats.write.mode("overwrite").csv("hdfs://namenode:9000/results/stats_summary")

spark.stop()