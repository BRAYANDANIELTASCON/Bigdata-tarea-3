Python
from pyspark.sql import SparkSession

# 1. Iniciar Spark
spark = SparkSession.builder.appName("TrabajoBigDataBatch").getOrCreate()

# 2. Crear los datos de ventas para el análisis
data = [
    ("001", "A", "Electronic accessories", 50.5, 3, 151.5),
    ("002", "B", "Health and beauty", 20.0, 5, 100.0),
    ("003", "A", "Food and beverages", 15.3, 10, 153.0),
    ("004", "C", "Electronic accessories", 100.0, 2, 200.0),
    ("005", "B", "Home and lifestyle", 45.0, 4, 180.0)
]
columns = ["Invoice_ID", "Branch", "Product_line", "Unit_Price", "Quantity", "Total"]

# 3. Cargar los datos en un DataFrame de Spark (Simula la carga del dataset)
df = spark.createDataFrame(data, columns)

print("=== DATOS CARGADOS Y LIMPIOS (EDA) ===")
df.show()

print("=== RESULTADO DEL ANALISIS BATCH (Ventas Totales por Categoria) ===")
# Agrupar por línea de producto y sumar las ventas
resultado = df.groupBy("Product_line").sum("Total")
resultado.show()