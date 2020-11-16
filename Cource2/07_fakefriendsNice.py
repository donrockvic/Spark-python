import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession 

# Creating the Spark Session
spark = SparkSession.builder.appName("FakeFriends").getOrCreate()
 
people = spark.read.option("header","true").option("inferSchema","true").csv("data/fakefriendsHeader.csv")
 
print("Here is our infered schema")
people.printSchema()

print("Name columns")
people.select("name").show()

print("filterout anyone <21")
people.filter(people.age <21).show()

print("group By Age")
people.groupBy("age").count().show()

print("Make everyone +10 year old")
people.select(people.name, people.age+10).show()

spark.stop()