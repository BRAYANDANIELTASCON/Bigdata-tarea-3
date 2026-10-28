================================================================================
  GUÍA DE REPLICACIÓN Y DESPLIEGUE - TAREA 3: APACHE SPARK & KAFKA (UNAD)
================================================================================
Autor: Brayan Tascon
Curso: Big Data - Tarea 3
Tecnologías: Apache Spark, Apache Kafka, Apache ZooKeeper, PySpark, Python 3

--------------------------------------------------------------------------------
1. DESCRIPCIÓN DEL PROYECTO
--------------------------------------------------------------------------------
Este proyecto implementa una arquitectura Big Data completa para el procesamiento
de datos de Netflix en dos modalidades:
  1. Procesamiento Batch (Por Lotes): Análisis exploratorio e histórico sobre un
     dataset de títulos de Netflix (netflix_titles.csv).
  2. Procesamiento en Streaming (Tiempo Real): Generación de eventos de usuarios
     con un Productor en Python -> Broker Apache Kafka -> Consumidor Spark Streaming.

--------------------------------------------------------------------------------
2. REQUISITOS DEL SISTEMA Y ENTORNO
--------------------------------------------------------------------------------
- Máquina Virtual Big Data (Ubuntu/Linux)
- Java OpenJDK 8 o 11 instalado
- Apache Kafka instalado en /opt/Kafka
- Apache Spark instalado en /opt/spark
- Python 3.x con librerías: kafka-python, pyspark
- Acceso SSH mediante PuTTY

--------------------------------------------------------------------------------
3. INSTRUCCIONES PASO A PASO PARA REPLICAR EL ENTORNO
--------------------------------------------------------------------------------

PASO 1: Configurar Variables de Entorno (En cada terminal)
---------------------------------------------------------
Abre tu terminal y exporta la ruta de Spark:

    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin


PASO 2: Iniciar Apache ZooKeeper (Pestaña 1 de PuTTY)
-----------------------------------------------------
ZooKeeper coordina el clúster de Kafka en el puerto 2181:

    sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties

* Nota: Mantén esta ventana abierta corriendo los logs.


PASO 3: Iniciar Apache Kafka (Pestaña 2 de PuTTY)
-------------------------------------------------
Kafka actúa como el broker de mensajes en el puerto 9092:

    sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties

* Nota: Mantén esta ventana abierta corriendo los logs.


PASO 4: Verificar Estado de los Servicios
-----------------------------------------
En una nueva pestaña de PuTTY, verifica que ZooKeeper y Kafka estén corriendo:

    sudo jps

Debes ver los procesos 'QuorumPeerMain' (ZooKeeper) y 'Kafka'.


PASO 5: Ejecutar el Procesamiento Batch (Opcional / Demostración 1)
------------------------------------------------------------------
Para procesar el dataset histórico en lotes:

    spark-submit netflix_batch.py


PASO 6: Iniciar el Productor de Streaming (Pestaña 3 de PuTTY)
--------------------------------------------------------------
Ejecuta el script que simula las reproducciones de usuarios y envía JSONs a Kafka:

    python3 netflix_producer.py

Verás mensajes continuos de [ENVIADO] Evento...


PASO 7: Iniciar el Consumidor Spark Streaming (Pestaña 4 de PuTTY)
------------------------------------------------------------------
En otra terminal, ejecuta el consumidor en tiempo real incluyendo el conector de Kafka:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py

Verás cómo Spark procesa y muestra en tablas de consola los micro-batches en vivo.


PASO 8: Monitoreo en Spark UI
-----------------------------
Abre tu navegador web e ingresa a:

    http://localhost:4040   (o la IP de la VM:4040)

Podrás inspeccionar:
- Pestaña 'Jobs': Grafo DAG y tareas ejecutadas.
- Pestaña 'Executors': Uso de memoria y cores.
- Pestaña 'Structured Streaming': Métricas de procesamiento y latencia en vivo.

================================================================================
¡Listo! Siguiendo estos 8 pasos tendrás todo el pipeline de Big Data desplegado.
================================================================================
