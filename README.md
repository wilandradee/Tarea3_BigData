Análisis de Datos con Spark y Kafka
Descripción
Este repositorio contiene un proyecto de análisis de datos en el que se realiza un procesamiento de datos utilizando Apache Spark y Apache Kafka. El trabajo se divide en dos partes:

Procesamiento de Datos con Spark: En esta sección, se realizan diversas preguntas sobre el conjunto de datos utilizando operaciones de Spark SQL y DataFrame.
Procesamiento en Tiempo Real con Spark y Kafka: Aquí se implementa un sistema de procesamiento de flujos en tiempo real utilizando Kafka para la ingesta de datos y Spark Streaming para el análisis. Se generan estadísticas de sensores en tiempo real, como temperatura y humedad.
Requisitos
Para ejecutar este proyecto, asegúrate de tener instalado lo siguiente:

Python 3
Apache Spark
Apache Kafka
Instalación
1. Clona este repositorio
2.Activar PySpark pyspark

3. Inicia Servidor ZeoKepper sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &

4.Servidor Kafka sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
