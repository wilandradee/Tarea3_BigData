#!/usr/bin/env python
# coding: utf-8




#Importar SparkSession
from pyspark.sql import *



#Crear SparkSession y SparkContext
spark = SparkSession.builder        .master("local[*]")        .appName('Spark_tarea3_Ejercicio_jupyter')        .getOrCreate()



spark



#creo la varible Path donde esta la ubicacion del  archivo
path="hdfs://localhost:9000/Tarea3/"
archivo="dataFamilia.csv"

#Se crea contexto

sc=spark.sparkContext


print("***************************            Uso de RDD           **********************")
#Se trabajara con RDDs
#Leer RDD
dataRDD_original = sc.textFile(path+archivo)     .map(lambda line : line.split(","))



#data_original
dataRDD_original.take(2)




#Revisamos el Primer Elemento y Eliminamos el encabezado
encabezado= dataRDD_original.first()
dataRDD = dataRDD_original.filter(lambda fila : fila!=encabezado)



#Conjunto Total de Filas
total_filas = dataRDD.count()
print("El total de registros del DataSet es de "+str(total_filas))


print("")
print("******             Verificar si hay datos nulos en el dataset             ***********")
#Verificamos Datos Nulos
cantidad_nulos = dataRDD.filter(lambda x : x is None).count()
print("La cantidad de Datos nulos es "+str(cantidad_nulos))
print("")


# Diferentes Tipos de Beneficiarios
print("*******************           Diferentes Tipos de Beneficiarios               **********************")
estados_beneficiarios=dataRDD.map(lambda fila: fila[4]).distinct().collect()
estados_beneficiarios
print(estados_beneficiarios)
print("")


print("*****************      Cantidad De beneficiarios en Estado Activo o No Activo        *****************")

#Mostrar la cantidad de beneficiarios sin el encabezado
for estado in estados_beneficiarios:
    cantidad = dataRDD.filter(lambda fila: fila[4] == estado).count()
    print("Existen "+str(cantidad)+" Beneficiarios con estado " +str(estado))
print("")


print("***********************        Busqueda agrupada por Departamentos(Fila por Fila) para determinar el numero de beneficiados         ***************")
#Se buscan entre los departamentos  
departamentos = dataRDD.map(lambda x: (x[9], 1))                              .reduceByKey(lambda a, b: a + b)

# Mostrar Deparamentos y la cantidad de beneficiarios por persona en el archivo
for departamento, cantidad in departamentos.collect():
    print(f"{departamento}: {cantidad} beneficiarios")
print("")




print("***********************      Los 5 Departamentos con el mayor numero de beneficiarios        *********************")

#Los 10 Departamentos con el mayor numero de beneficiarios
departamentos_ordenados_beneciarios_descendente=departamentos.sortBy(lambda filas : filas[1],ascending=False)
top_departamentos=departamentos_ordenados_beneciarios_descendente.take(5)
print(top_departamentos)
print("")



#Contar Cuantos beneficiarios se Inscribieron en el año 2018
print("********************    Contar Cuantos beneficiarios se Inscribieron en el año 2018     ***********************")

from datetime import datetime

columna = 6
año=2018

fecha_inicial=datetime.strptime(str(año)+"-01-01", "%Y-%m-%d")
fecha_final=datetime.strptime(str(año)+"-12-31", "%Y-%m-%d")
numeros_inscritos_año=dataRDD.filter(lambda fila :
                                    datetime.strptime(fila[columna], "%Y-%m-%d")>=fecha_inicial and
                                    datetime.strptime(fila[columna], "%Y-%m-%d")<=fecha_final
                                    )

n_inscritos=numeros_inscritos_año.count()
print("Se incribieron alrededor de "+str(n_inscritos)+" al programa durante al año "+str(año))
print("")
print("")



# In[16]:
print("*************************     Creacion de Dataframe a partir del RDD anterior      *******************************")

#Crear Dataframe a partir del RDD dataRDD

df = spark.createDataFrame(dataRDD, schema=encabezado)
#df.printSchema()




print("")


print("")
#Ver Columnas
print("****************           Ver Algunas Columnas del DataFrame *******************************")
df.select("Bancarizado","Discapacidad","EstadoBeneficiario","FechaInscripcionBeneficiario").show(4)
print("")


# In[28]:

print("*****************      Cantidad de Beneficiarios segun la Columna Cantidad de Beneficiarios, agrupada por el departamento      **********")
print("")
#Filtraremos ahora la cantidad de beneficiarios y por el departamento, en este caso se tomara la fila de cantidad de benficiarios
#Para definir el numero de personas en el nucleo familiar que beneficia el programa agrupados por departamento
from pyspark.sql.functions import count, sum, max, min, avg, col

df = df.withColumn("CantidadDeBeneficiarios", col("CantidadDeBeneficiarios").cast("int"))

datos=(df.groupBy("NombreDepartamentoAtencion")
    .agg(
        count("CantidadDeBeneficiarios").alias("count"),
        sum("CantidadDeBeneficiarios").alias("sum"),
        max("CantidadDeBeneficiarios").alias("max"),
        min("CantidadDeBeneficiarios").alias("min"),
        avg("CantidadDeBeneficiarios").alias("avg")
        )
)



datos.orderBy(col("count").desc()).show()