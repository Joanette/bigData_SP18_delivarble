#Code here
#Find all the schools in the city of Ponce or in Cabo Rojo or in Dorado. 
from pyspark import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

sqlContext = SQLContext(sc)
schemaESC = StructType([
				StructField("regionEducativa", StringType(), True),
				StructField("distritoEscolar", StringType(), True),
                StructField("ciudad", StringType(), True),
				StructField("schoolid", IntegerType(), True),
				StructField("schoolName", StringType(), True),
				StructField("nivel", StringType(), True),
				StructField("CBid",IntegerType(), True),
				]
			)
schemaStudents = StructType([
				StructField("regionEducativa", StringType(), True),
				StructField("distritoEscolar", StringType(), True),
         		StructField("schoolid", IntegerType(), True),
				StructField("schoolName", StringType(), True),
				StructField("nivel", StringType(), True),
				StructField("sexo",StringType(), True),
				StructField("studentid",IntegerType(), True),
				]
			)
escuelaPRdf = sqlContext.read\
			 .options(header='true')\
			 .schema(schemaESC)\
			 .csv('escuelasPR.csv')
estudiantesdf = sqlContext.read\
			 .options(header='true')\
			 .schema(schemaStudents)\
			 .csv('studentsPR.csv')

escuelaPRdf.filter((col("ciudad") == "Ponce") | (col("ciudad") == "Cabo Rojo") |(col("ciudad") == "Dorado") ).show()
