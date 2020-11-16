import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FakeFiends")
sc = SparkContext(conf=conf)
# sc = SparkContext("local","RatingCounts")

def parseLine(line):
	fileds = line.split(',')
	age = int(fileds[2])
	numFriends = int(fileds[3])
	return (age, numFriends)


lines = sc.textFile("data/fakefriends.csv")
rdd = lines.map(parseLine)

totalByAge = rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

averageByAge = totalByAge.mapValues(lambda x: x[0]/x[1])

results = averageByAge.collect()
for result in results:
	print(result)