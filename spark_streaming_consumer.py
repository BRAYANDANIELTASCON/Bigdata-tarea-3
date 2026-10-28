from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

def main():
    print("==================================================")
    print("  INICIANDO CONSUMIDOR SPARK STRUCTURED STREAMING")
    print("==================================================")

    # Crear la sesión de Spark
    spark = SparkSession.builder \
        .appName("Netflix Streaming Consumer - UNAD Tarea 3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Definir el esquema JSON coincidente con el productor
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("action", StringType(), True),
        StructField("country", StringType(), True),
        StructField("timestamp", LongType(), True)
    ])

    # Leer el flujo continuo desde Apache Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "netflix_streams") \
        .option("startingOffsets", "latest") \
        .load()

    # Convertir el valor binario de Kafka a STRING y parsear el JSON
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*")

    # Agregar métricas en tiempo real: Conteo de acciones por título
    streaming_counts = json_df.groupBy("title", "action") \
                              .count() \
                              .orderBy(col("count").desc())

    # Escribir el resultado formateado en la consola en micro-batches
    query = streaming_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("[INFO] Consumidor escuchando eventos en tiempo real en el tópico 'netflix_streams'...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
