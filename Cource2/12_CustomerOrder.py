import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
# Creating the Spark Session
spark = SparkSession.builder.appName("Orders").getOrCreate()

schema = StructType([StructField("customerId",IntegerType(),True), StructField("OrderId",StringType(),True),  StructField("price",FloatType(), True)])

lines = spark.read.schema(schema).csv("data/customer-orders.csv")
lines.printSchema()
data = lines.select("customerId","price")
# data.groupBy("customerId").sum("price").show()
 
totalOrder = data.groupBy("customerId").agg(func.round(func.sum("price"),2).alias("total_spent"))

totalOrderSorted = totalOrder.sort("total_spent")

totalOrderSorted.show(totalOrderSorted.count())
spark.stop()