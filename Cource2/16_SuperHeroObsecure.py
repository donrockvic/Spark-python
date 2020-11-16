import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
import codecs


# Creating the Spark Session
spark = SparkSession.builder.appName("SuperHero").getOrCreate()

schema = StructType([StructField("id",IntegerType(), True), StructField("name",StringType(),True)])

names = spark.read.schema(schema).option("sep"," ").csv("data/Marvel-names.txt")

lines = spark.read.text("data/Marvel-graph.txt")

connections = lines.withColumn("id",func.split(func.col("value"), " ")[0]).withColumn("connections",func.size(func.split(func.col("value"), " "))-1).groupBy("id").agg(func.sum("connections").alias("connections"))
minConnectionCount = connections.agg(func.min("connections")).first()[0]
minConnection = connections.filter(func.col("connections")==minConnectionCount)
minConnectionName = minConnection.join(names,"id")

minConnectionName.show()

spark.stop()