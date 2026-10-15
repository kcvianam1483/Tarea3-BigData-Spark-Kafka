# Tarea3-BigData-Spark-Kafka
# Tarea 3: Procesamiento de Datos Masivos con Apache Spark y Apache Kafka

**Curso:** Big Data  
**Institución:** Universidad Nacional Abierta y a Distancia (UNAD)  
**Programa:** Ingeniería de Sistemas  
**Dataset:** Presión Atmosférica en Colombia - IDEAM (> 1.19 GB)  

## 📌 Descripción del Proyecto
Este proyecto implementa una arquitectura híbrida de Big Data para el procesamiento masivo de datos climáticos históricos y en tiempo real provistos por el **IDEAM** (Instituto de Hidrología, Meteorología y Estudios Ambientales de Colombia).

La solución aborda dos paradigmas fundamentales del procesamiento distribuido:
1. **Procesamiento en Lote (Batch):** Ingesta del conjunto de datos (> 1.19 GB) en **Hadoop HDFS**, seguido de un proceso de depuración, estandarización (*Data Quality*) y agregación estadística distribuida utilizando **PySpark SQL**.
2. **Procesamiento en Tiempo Real (Streaming):** Transmisión continua de datos meteorológicos mediante **Apache Kafka** (Topic: `presion_data`) y procesamiento por ventanas temporales utilizando **Spark Structured Streaming**.

---

## 🛠️ Tecnologías y Herramientas Utilizadas

* **Sistema de Archivos Distribuido:** Apache Hadoop HDFS
* **Motor de Procesamiento Batch & Streaming:** Apache Spark 3.5.x / PySpark
* **Gestor de Mensajería en Tiempo Real:** Apache Kafka & Apache ZooKeeper
* **Lenguaje de Programación:** Python 3.x
* **Librerías:** `pyspark`, `kafka-python`, `matplotlib`, `pandas`
* **Entorno de Ejecución:** Linux Ubuntu (VirtualBox VM) / PuTTY

---

## 📁 Estructura del Repositorio
```text
├── scripts/
│   ├── analisis_batch.py                    # Script de procesamiento batch y limpieza en PySpark
│   ├── kafka_producer.py                    # Productor de eventos en tiempo real hacia Kafka
│   └── spark_streaming_consumer.py          # Consumidor de Spark Structured Streaming
├── docs/
│   └── grafico_presion_departamentos.png    # Visualización de presión promedio por departamento
└── README.md                                # Documentación principal del proyecto
