{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Importar SparkSession\n",
    "from pyspark.sql import *\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Crear SparkSession y SparkContext\n",
    "spark = SparkSession.builder\\\n",
    "        .master(\"local[*]\")\\\n",
    "        .appName('Spark_tarea3_Ejercicio_jupyter')\\\n",
    "        .getOrCreate()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "\n",
       "            <div>\n",
       "                <p><b>SparkSession - in-memory</b></p>\n",
       "                \n",
       "        <div>\n",
       "            <p><b>SparkContext</b></p>\n",
       "\n",
       "            <p><a href=\"http://10.255.255.254:4040\">Spark UI</a></p>\n",
       "\n",
       "            <dl>\n",
       "              <dt>Version</dt>\n",
       "                <dd><code>v3.5.3</code></dd>\n",
       "              <dt>Master</dt>\n",
       "                <dd><code>local[*]</code></dd>\n",
       "              <dt>AppName</dt>\n",
       "                <dd><code>Spark_tarea3_Ejercicio_jupyter</code></dd>\n",
       "            </dl>\n",
       "        </div>\n",
       "        \n",
       "            </div>\n",
       "        "
      ],
      "text/plain": [
       "<pyspark.sql.session.SparkSession at 0x7ff04c0294d0>"
      ]
     },
     "execution_count": 3,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "spark"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [],
   "source": [
    "#creo la varible Path donde esta la ubicacion del  archivo \n",
    "path=\"/home/hugo/EJEMPLO_SPARK/SPARK_Guia_CURSO/Data/data_tarea3_FAccion/\"\n",
    "archivo=\"data.csv\"\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Se crea contexto\n",
    "\n",
    "sc=spark.sparkContext"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Se trabajara con RDDs\n",
    "#Leer RDD\n",
    "dataRDD_original = sc.textFile(path+archivo) \\\n",
    "    .map(lambda line : line.split(\",\"))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[['Bancarizado',\n",
       "  'CodigoDepartamentoAtencion',\n",
       "  'CodigoMunicipioAtencion',\n",
       "  'Discapacidad',\n",
       "  'EstadoBeneficiario',\n",
       "  'Etnia',\n",
       "  'FechaInscripcionBeneficiario',\n",
       "  'Genero',\n",
       "  'NivelEscolaridad',\n",
       "  'NombreDepartamentoAtencion',\n",
       "  'NombreMunicipioAtencion',\n",
       "  'Pais',\n",
       "  'TipoAsignacionBeneficio',\n",
       "  'TipoBeneficio',\n",
       "  'TipoDocumento',\n",
       "  'TipoPoblacion',\n",
       "  'RangoBeneficioConsolidadoAsignado',\n",
       "  'RangoUltimoBeneficioAsignado',\n",
       "  'FechaUltimoBeneficioAsignado',\n",
       "  'RangoEdad',\n",
       "  'Titular',\n",
       "  'CantidadDeBeneficiarios'],\n",
       " ['SI',\n",
       "  '08',\n",
       "  '08421',\n",
       "  'NO',\n",
       "  'ACTIVO',\n",
       "  'AFROCOLOMBIANO – NEGRO',\n",
       "  '2012-12-01',\n",
       "  'Hombre',\n",
       "  'ND',\n",
       "  'ATLANTICO',\n",
       "  'LURUACO',\n",
       "  'ND',\n",
       "  'MONETARIO',\n",
       "  'ND',\n",
       "  'CC',\n",
       "  'UNIDOS',\n",
       "  '4.500.001 - 6.000.000',\n",
       "  '0 - 1.300.000',\n",
       "  '2018-01-01',\n",
       "  '30-49',\n",
       "  'SI',\n",
       "  '1']]"
      ]
     },
     "execution_count": 7,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#data_original\n",
    "dataRDD_original.take(2)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Revisamos el Primer Elemento y Eliminamos el encabezado\n",
    "encabezado= dataRDD_original.first()\n",
    "dataRDD = dataRDD_original.filter(lambda fila : fila!=encabezado)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "3958594"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Conjunto Total de Filas\n",
    "total_filas = dataRDD.count()\n",
    "total_filas"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "0"
      ]
     },
     "execution_count": 10,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Verificamos Datos Nulos\n",
    "dataRDD.filter(lambda x : x is None).count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['ACTIVO', 'NO ACTIVO']"
      ]
     },
     "execution_count": 11,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Diferentes Tipos de Beneficiarios\n",
    "estados_beneficiarios=dataRDD.map(lambda fila: fila[4]).distinct().collect()\n",
    "estados_beneficiarios"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Existen 3576756 Beneficiarios con estado ACTIVO\n",
      "Existen 381838 Beneficiarios con estado NO ACTIVO\n"
     ]
    }
   ],
   "source": [
    "#Mostrar la cantidad de beneficiarios sin el encabezado\n",
    "for estado in estados_beneficiarios:\n",
    "    cantidad = dataRDD.filter(lambda fila: fila[4] == estado).count()\n",
    "    print(\"Existen \"+str(cantidad)+\" Beneficiarios con estado \" +str(estado))\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "GUAVIARE: 15248 beneficiarios\n",
      "BOYACA: 104177 beneficiarios\n",
      "ANTIOQUIA: 475128 beneficiarios\n",
      "SANTANDER: 157760 beneficiarios\n",
      "SAN ANDRES: 4013 beneficiarios\n",
      "META: 102982 beneficiarios\n",
      "NARIÑO: 221933 beneficiarios\n",
      "CESAR: 157620 beneficiarios\n",
      "QUINDIO: 33663 beneficiarios\n",
      "MAGDALENA: 188629 beneficiarios\n",
      "ARAUCA: 40133 beneficiarios\n",
      "RISARALDA: 63210 beneficiarios\n",
      "CHOCO: 82568 beneficiarios\n",
      "ATLANTICO: 166741 beneficiarios\n",
      "NORTE DE SANTANDER: 141884 beneficiarios\n",
      "BOGOTA: 85467 beneficiarios\n",
      "VICHADA: 5863 beneficiarios\n",
      "CORDOBA: 256785 beneficiarios\n",
      "AMAZONAS: 8600 beneficiarios\n",
      "GUAINIA: 4232 beneficiarios\n",
      "CUNDINAMARCA: 162035 beneficiarios\n",
      "SUCRE: 141566 beneficiarios\n",
      "CALDAS: 68789 beneficiarios\n",
      "CAQUETA: 73428 beneficiarios\n",
      "LA GUAJIRA: 93986 beneficiarios\n",
      "PUTUMAYO: 64076 beneficiarios\n",
      "VAUPES: 2882 beneficiarios\n",
      "VALLE: 222565 beneficiarios\n",
      "TOLIMA: 158562 beneficiarios\n",
      "BOLIVAR: 245825 beneficiarios\n",
      "HUILA: 148231 beneficiarios\n",
      "CASANARE: 55035 beneficiarios\n",
      "CAUCA: 204978 beneficiarios\n"
     ]
    }
   ],
   "source": [
    "#Se buscan entre los departamentos  \n",
    "departamentos = dataRDD.map(lambda x: (x[9], 1)) \\\n",
    "                             .reduceByKey(lambda a, b: a + b)\n",
    "\n",
    "# Mostrar Deparamentos y la cantidad de beneficiarios por persona en el archivo\n",
    "for departamento, cantidad in departamentos.collect():\n",
    "    print(f\"{departamento}: {cantidad} beneficiarios\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[('ANTIOQUIA', 475128),\n",
       " ('CORDOBA', 256785),\n",
       " ('BOLIVAR', 245825),\n",
       " ('VALLE', 222565),\n",
       " ('NARIÑO', 221933)]"
      ]
     },
     "execution_count": 14,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Los 5 Departamentos con el mayor numero de beneficiarios\n",
    "departamentos_ordenados_beneciarios_descendente=departamentos.sortBy(lambda filas : filas[1],ascending=False)\n",
    "departamentos_ordenados_beneciarios_descendente.take(5)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "68810"
      ]
     },
     "execution_count": 15,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Contar Cuantos beneficiarios se Inscribieron en el año 2018\n",
    "from datetime import datetime\n",
    "\n",
    "columna = 6\n",
    "año=2018\n",
    "\n",
    "fecha_inicial=datetime.strptime(str(año)+\"-01-01\", \"%Y-%m-%d\")\n",
    "fecha_final=datetime.strptime(str(año)+\"-12-31\", \"%Y-%m-%d\")\n",
    "numeros_inscritos_año=dataRDD.filter(lambda fila :\n",
    "                                    datetime.strptime(fila[columna], \"%Y-%m-%d\")>=fecha_inicial and\n",
    "                                    datetime.strptime(fila[columna], \"%Y-%m-%d\")<=fecha_final\n",
    "                                    )\n",
    "\n",
    "numeros_inscritos_año.count()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "root\n",
      " |-- Bancarizado: string (nullable = true)\n",
      " |-- CodigoDepartamentoAtencion: string (nullable = true)\n",
      " |-- CodigoMunicipioAtencion: string (nullable = true)\n",
      " |-- Discapacidad: string (nullable = true)\n",
      " |-- EstadoBeneficiario: string (nullable = true)\n",
      " |-- Etnia: string (nullable = true)\n",
      " |-- FechaInscripcionBeneficiario: string (nullable = true)\n",
      " |-- Genero: string (nullable = true)\n",
      " |-- NivelEscolaridad: string (nullable = true)\n",
      " |-- NombreDepartamentoAtencion: string (nullable = true)\n",
      " |-- NombreMunicipioAtencion: string (nullable = true)\n",
      " |-- Pais: string (nullable = true)\n",
      " |-- TipoAsignacionBeneficio: string (nullable = true)\n",
      " |-- TipoBeneficio: string (nullable = true)\n",
      " |-- TipoDocumento: string (nullable = true)\n",
      " |-- TipoPoblacion: string (nullable = true)\n",
      " |-- RangoBeneficioConsolidadoAsignado: string (nullable = true)\n",
      " |-- RangoUltimoBeneficioAsignado: string (nullable = true)\n",
      " |-- FechaUltimoBeneficioAsignado: string (nullable = true)\n",
      " |-- RangoEdad: string (nullable = true)\n",
      " |-- Titular: string (nullable = true)\n",
      " |-- CantidadDeBeneficiarios: string (nullable = true)\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#Crear Dataframe a partir del RDD dataRDD\n",
    "\n",
    "df = spark.createDataFrame(dataRDD, schema=encabezado)\n",
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['Bancarizado',\n",
       " 'CodigoDepartamentoAtencion',\n",
       " 'CodigoMunicipioAtencion',\n",
       " 'Discapacidad',\n",
       " 'EstadoBeneficiario',\n",
       " 'Etnia',\n",
       " 'FechaInscripcionBeneficiario',\n",
       " 'Genero',\n",
       " 'NivelEscolaridad',\n",
       " 'NombreDepartamentoAtencion',\n",
       " 'NombreMunicipioAtencion',\n",
       " 'Pais',\n",
       " 'TipoAsignacionBeneficio',\n",
       " 'TipoBeneficio',\n",
       " 'TipoDocumento',\n",
       " 'TipoPoblacion',\n",
       " 'RangoBeneficioConsolidadoAsignado',\n",
       " 'RangoUltimoBeneficioAsignado',\n",
       " 'FechaUltimoBeneficioAsignado',\n",
       " 'RangoEdad',\n",
       " 'Titular',\n",
       " 'CantidadDeBeneficiarios']"
      ]
     },
     "execution_count": 18,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Ver Columnas\n",
    "df.columns"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Filtraremos ahora la cantidad de beneficiarios y por el departamento, en este caso se tomara la fila de cantidad de benficiarios\n",
    "#Para definir el numero de personas en el nucleo familiar que beneficia el programa agrupados por departamento\n",
    "from pyspark.sql.functions import count, sum, max, min, avg, col\n",
    "\n",
    "df = df.withColumn(\"CantidadDeBeneficiarios\", col(\"CantidadDeBeneficiarios\").cast(\"int\"))\n",
    "\n",
    "datos=(df.groupBy(\"NombreDepartamentoAtencion\")\n",
    "    .agg(\n",
    "        count(\"CantidadDeBeneficiarios\").alias(\"count\"),\n",
    "        sum(\"CantidadDeBeneficiarios\").alias(\"sum\"),\n",
    "        max(\"CantidadDeBeneficiarios\").alias(\"max\"),\n",
    "        min(\"CantidadDeBeneficiarios\").alias(\"min\"),\n",
    "        avg(\"CantidadDeBeneficiarios\").alias(\"avg\")\n",
    "        )\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "+--------------------------+------+-------+-----+---+------------------+\n",
      "|NombreDepartamentoAtencion| count|    sum|  max|min|               avg|\n",
      "+--------------------------+------+-------+-----+---+------------------+\n",
      "|                 ANTIOQUIA|475128|1170760| 6420|  1| 2.464093886279066|\n",
      "|                   CORDOBA|256785| 724978| 5440|  1| 2.823287964639679|\n",
      "|                   BOLIVAR|245825| 694730| 4661|  1|2.8261161395301535|\n",
      "|                     VALLE|222565| 592945| 4663|  1| 2.664143059330982|\n",
      "|                    NARIÑO|221933| 560698| 3434|  1|2.5264291475355174|\n",
      "|                     CAUCA|204978| 507296| 1151|  1|2.4748802310491858|\n",
      "|                 MAGDALENA|188629| 514243| 3380|  1| 2.726213890759109|\n",
      "|                 ATLANTICO|166741| 490862| 4735|  1|2.9438590388686645|\n",
      "|              CUNDINAMARCA|162035| 357785|  895|  1| 2.208072330052149|\n",
      "|                    TOLIMA|158562| 388281| 2181|  1| 2.448764521133689|\n",
      "|                 SANTANDER|157760| 387723| 1198|  1|2.4576762170385393|\n",
      "|                     CESAR|157620| 397745| 2761|  1|2.5234424565410483|\n",
      "|                     HUILA|148231| 380170| 1410|  1| 2.564713184151763|\n",
      "|        NORTE DE SANTANDER|141884| 388713| 7709|  1| 2.739653519776719|\n",
      "|                     SUCRE|141566| 362836| 2151|  1| 2.563016543520337|\n",
      "|                    BOYACA|104177| 242600|  592|  1|2.3287289900841834|\n",
      "|                      META|102982| 243316| 3060|  1|2.3627041618923696|\n",
      "|                LA GUAJIRA| 93986| 265362| 1651|  1|2.8234205094375757|\n",
      "|                    BOGOTA| 85467| 371592|14701|  1| 4.347783354979115|\n",
      "|                     CHOCO| 82568| 180095|  794|  1|2.1811718825695183|\n",
      "+--------------------------+------+-------+-----+---+------------------+\n",
      "only showing top 20 rows\n",
      "\n"
     ]
    }
   ],
   "source": [
    "datos.orderBy(col(\"count\").desc()).show()\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}