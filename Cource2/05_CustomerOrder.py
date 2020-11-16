import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')
from pyspark import SparkConf, SparkContext
import re
import collections

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf=conf)

def dataClean(line):
	line = line.split(',')
	customer = int(line[0])
	amount = float(line[2])
	return (customer, amount)

lines = sc.textFile("data/customer-orders.csv")
data = lines.map(dataClean)
results = data.reduceByKey(lambda x,y:x+y)

def reverse(x):
	return (x[1], x[0])

flipped = results.map(reverse)
sortedResult = flipped.sortByKey()
results = sortedResult.collect()

for result in results:
	print(result)