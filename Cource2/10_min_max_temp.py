import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
# Creating the Spark Session
spark = SparkSession.builder.appName("MinMaxTemp").getOrCreate()

def parseLine(line):
	fileds = line.split(',')
	stationID = fileds[0]
	entryType = fileds[2]
	temp = float(fileds[3])*0.1*(9.0/5.0)+32.0
	return Row(stationID=str(stationID), entryType=str(entryType), temp=float(temp)) 

lines = spark.sparkContext.textFile("data/1800.csv")
Data = lines.map(parseLine)

# Infer the schema and register the Dataframe as a table
schemaData = spark.createDataFrame(Data).cache()
schemaData.createOrReplaceTempView("Weather")

dataPlace = schemaData.filter(schemaData.entryType == 'TMIN')
dataPlace = dataPlace.groupBy("stationID").min("temp")
dataPlace.show()

dataPlace = schemaData.filter(schemaData.entryType == 'TMAX')
dataPlace = dataPlace.groupBy("stationID").max("temp")
dataPlace.show()
 
spark.stop()