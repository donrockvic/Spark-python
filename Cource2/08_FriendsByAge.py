import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession 
from pyspark.sql import functions as func
# Creating the Spark Session
spark = SparkSession.builder.appName("FakeFriends").getOrCreate()
 
people = spark.read.option("header","true").option("inferSchema","true").csv("data/fakefriendsHeader.csv")
 
print("Here is our infered schema")
people.printSchema()

print("select onyl age and numfirnds")
friendsByAge = people.select("age","friends")

friendsByAge.groupBy("age").avg("friends").show()

friendsByAge.groupBy("age").avg("friends").sort("age").show()

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"),2)).sort("age").show()

friendsByAge.groupBy("age").agg(func.round(func.avg("friends"),2).alias("Friends_avg")).sort("age").show()


spark.stop()