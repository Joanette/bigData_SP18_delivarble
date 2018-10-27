#Code here
# Find all the female students enrolled in school with id 71381
from pyspark import SparkContext 
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

sqlContext = SQLContext(sc)

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

estudiantesdf = sqlContext.read\
			 .options(header='true')\
			 .schema(schemaStudents)\
			 .csv('studentsPR.csv')

df4 = estudiantesdf.filter(estudiantesdf.sexo =='F')
df4.filter(df4.schoolid == 71381).show()
