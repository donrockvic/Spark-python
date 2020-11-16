import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
# Creating the Spark Session
spark = SparkSession.builder.appName("Orders").getOrCreate()

schema = StructType([StructField("userID",IntegerType(),True), StructField("movieID",IntegerType(),True),StructField("rating",FloatType(), True),StructField("timeStamp",LongType(), True)])

lines = spark.read.option("sep","\t").schema(schema).csv("data/ml-100k/u.data")
lines.printSchema()
data = lines.groupBy("movieID").count().orderBy(func.desc("count"))
data.show(10)
spark.stop()