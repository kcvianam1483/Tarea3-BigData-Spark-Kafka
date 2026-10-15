from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp, upper, translate, when, trim
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingPresion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir el esquema con los nombres EXACTOS en MINÚSCULAS
schema = StructType([
    StructField("codigoestacion", StringType()),
    StructField("fechaobservacion", StringType()),
    StructField("valorobservado", StringType()),
    StructField("nombreestacion", StringType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType())
])

# 3. Leer los datos en streaming desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "presion_data") \
    .load()

# 4. Parsear el JSON que envía el productor
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5. Limpieza en tiempo real (Calidad de Datos Avanzada)
clean_df = parsed_df.withColumn("valorobservado", col("valorobservado").cast(FloatType()))

# Normalización: Mayúsculas, sin tildes, sin Ñ y sin espacios (trim)
clean_df = clean_df.withColumn("departamento", trim(upper(col("departamento"))))
clean_df = clean_df.withColumn("departamento", translate(col("departamento"), "ÁÉÍÓÚÑ", "AEIOUN"))

# Tratamiento de Nulos y textos corruptos
clean_df = clean_df.filter(
    col("valorobservado").isNotNull() & 
    col("departamento").isNotNull() & 
    (col("departamento") != "") & 
    (col("departamento") != "NULL") &
    (col("departamento") != "SIN DATO") &
    (col("departamento") != "<NIL>")
)

# Unificar variantes de Bogotá
clean_df = clean_df.withColumn(
    "departamento", 
    when(col("departamento").like("%BOGOTA%"), "BOGOTA")
    .otherwise(col("departamento"))
)

# Filtrar valores atípicos (entre 500 y 1100 hPa)
clean_df = clean_df.filter((col("valorobservado") > 500) & (col("valorobservado") < 1100))

# 6. Procesamiento en tiempo real (ventana de 1 minuto)
windowed_stats = clean_df \
    .withColumn("Procesamiento", current_timestamp()) \
    .groupBy(window(col("Procesamiento"), "1 minute"), "departamento") \
    .agg({"valorobservado": "avg"}) \
    .withColumnRenamed("avg(valorobservado)", "PresionPromedio") \
    .withColumnRenamed("departamento", "Departamento")

# 7. Imprimir los resultados en la consola
query = windowed_stats \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
