import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

def main():
    # Inicializar la sesión de Spark
    spark = SparkSession.builder \
        .appName("Netflix Batch Processing - UNAD Tarea 3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("\n==================================================")
    print("  INICIANDO PROCESAMIENTO BATCH CON APACHE SPARK")
    print("==================================================\n")

    # Ruta del archivo CSV de Netflix
    csv_path = "netflix_titles.csv"

    # Leer el dataset de Netflix
    try:
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
    except Exception as e:
        print(f"Error leyendo el archivo {csv_path}: {e}")
        sys.exit(1)

    print("--- Esquema del Dataset ---")
    df.printSchema()

    # Limpieza básica: Eliminar registros sin tipo ni título
    df_clean = df.filter(col("title").isNotNull() & col("type").isNotNull())

    print("\n--- Conteo de Contenido por Tipo (Movie vs TV Show) ---")
    type_counts = df_clean.groupBy("type").count().orderBy(desc("count"))
    type_counts.show()

    print("\n--- Top 10 Países con Más Producciones ---")
    country_counts = df_clean.filter(col("country").isNotNull()) \
                             .groupBy("country") \
                             .count() \
                             .orderBy(desc("count"))
    country_counts.show(10, truncate=False)

    print("\n--- Distribución de Producciones por Año de Lanzamiento (Últimos 10 años) ---")
    year_counts = df_clean.filter(col("release_year").isNotNull()) \
                           .groupBy("release_year") \
                           .count() \
                           .orderBy(desc("release_year"))
    year_counts.show(10)

    print("\n==================================================")
    print("  PROCESAMIENTO BATCH FINALIZADO EXITOSAMENTE")
    print("==================================================\n")

    spark.stop()

if __name__ == "__main__":
    main()
