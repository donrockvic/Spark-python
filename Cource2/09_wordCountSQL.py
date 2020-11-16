import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')
import re
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func

# Creating the Spark Session
spark = SparkSession.builder.appName("CountWords").getOrCreate()
 
lines = spark.read.text("data/book.txt")

words = lines.select(func.explode(func.split(lines.value, "\\W+")).alias("word"))
words.filter(words.word != "")

lowerWords = words.select(func.lower(words.word).alias("word"))

wordCounts = lowerWords.groupBy("word").count()

wordCountsSort = wordCounts.sort("count")

wordCountsSort.show(wordCountsSort.count())

spark.stop()