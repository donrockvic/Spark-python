import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
# Creating the Spark Session
spark = SparkSession.builder.appName("MinMaxTemp").getOrCreate()

schema = StructType([StructField("stationId",StringType(),True), StructField("date",IntegerType(),True), StructField("measure_type",StringType(), True), StructField("temperature",FloatType(), True)])

lines = spark.read.schema(schema).csv("data/1800.csv")
lines.printSchema()

minTemp = lines.filter(lines.measure_type == 'TMIN')
stationsMin = minTemp.select("stationId","temperature")

minStations = stationsMin.groupBy("stationId").min("temperature")
minStations.show()

minStationsF = minStations.withColumn("temperature",func.round(func.col("min(temperature)")*0.1*(9.0/5.0)+32.0)).select("stationId","temperature").sort("temperature")

results = minStationsF.collect()
for result in results:
	print(result)

# sorted


maxTemp = lines.filter(lines.measure_type == 'TMAX')
stationsMax = maxTemp.select("stationId","temperature")
maxStations = stationsMax.groupBy("stationId").max("temperature")
maxStations.show()

maxStationsF = maxStations.withColumn("temperature",func.round(func.col("max(temperature)")*0.1*(9.0/5.0)+32.0)).select("stationId","temperature").sort("temperature")

results = maxStationsF.collect()
for result in results:
	print(result)

spark.stop()