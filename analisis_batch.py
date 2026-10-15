import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, upper, translate, when, trim
import pandas as pd
import matplotlib.pyplot as plt

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisBatchPresion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("--- 1. Cargando datos desde la API ---")
url_api = "https://www.datos.gov.co/resource/62tk-nxj5.csv?$limit=100000"
archivo_temporal = "dataset_temporal.csv"

req = urllib.request.Request(url_api, headers={'User-Agent': 'Mozilla/5.0'})
with urllib.request.urlopen(req) as respuesta, open(archivo_temporal, 'wb') as archivo:
    archivo.write(respuesta.read())

df = spark.read.csv(archivo_temporal, header=True, inferSchema=True)

print("--- 2. Limpieza y Transformación de datos (Calidad de Datos Avanzada) ---")
# 1. Casteo numérico
df_clean = df.withColumn("valorobservado", col("valorobservado").cast("float"))

# 2. Normalización de textos: Mayúsculas, sin tildes, sin Ñ y sin espacios a los lados (trim)
df_clean = df_clean.withColumn("departamento", trim(upper(col("departamento"))))
df_clean = df_clean.withColumn("departamento", translate(col("departamento"), "ÁÉÍÓÚÑ", "AEIOUN"))

df_clean = df_clean.withColumn("municipio", trim(upper(col("municipio"))))
df_clean = df_clean.withColumn("municipio", translate(col("municipio"), "ÁÉÍÓÚÑ", "AEIOUN"))

# 3. Tratamiento de Nulos y textos corruptos (incluyendo <NIL>)
df_clean = df_clean.filter(
    col("valorobservado").isNotNull() & 
    col("departamento").isNotNull() & 
    (col("departamento") != "") & 
    (col("departamento") != "NULL") &
    (col("departamento") != "SIN DATO") &
    (col("departamento") != "<NIL>")
)

# 4. Unificar las variantes de Bogotá
df_clean = df_clean.withColumn(
    "departamento", 
    when(col("departamento").like("%BOGOTA%"), "BOGOTA")
    .otherwise(col("departamento"))
)

# 5. Filtrar valores atípicos (Sensores dañados o errores de digitación)
# Se conservan únicamente las presiones lógicas en la Tierra (entre 500 y 1100 hPa)
df_clean = df_clean.filter((col("valorobservado") > 500) & (col("valorobservado") < 1100))

print("--- 3. Análisis Exploratorio de Datos (EDA) ---")
eda_df = df_clean.groupBy("departamento") \
    .agg(
        avg("valorobservado").alias("presion_promedio"),
        max("valorobservado").alias("presion_maxima"),
        min("valorobservado").alias("presion_minima")
    ) \
    .orderBy("presion_promedio", ascending=False)

print("--- 4. Visualización Tabular ---")
eda_df.show(50, truncate=False)

print("--- 5. Generando Visualización Gráfica ---")
# Convertimos el resultado de Spark a Pandas para poder graficarlo
pdf = eda_df.toPandas()

# Crear un gráfico de barras
plt.figure(figsize=(12, 6))
plt.bar(pdf["departamento"], pdf["presion_promedio"], color="skyblue", edgecolor="black")
plt.xticks(rotation=90, fontsize=8)
plt.ylabel("Presión Atmosférica Promedio (hPa)")
plt.xlabel("Departamento")
plt.title("Presión Atmosférica Promedio por Departamento en Colombia")
plt.tight_layout()

# Guardar el gráfico como imagen en la máquina virtual
plt.savefig("visualizacion_presion.png")
print("¡Éxito! Gráfico guardado en la carpeta actual como 'visualizacion_presion.png'")

spark.stop()



