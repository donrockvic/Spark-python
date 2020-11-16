import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession, Row

# Creating the Spark Session
spark = SparkSession.builder.appName("FakeFriends").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("data/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema and register the Dataframe as a table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL
teens = spark.sql("SELECT * FROM PEOPLE WHERE AGE >= 13 AND AGE <=19")

for teen in teens.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()