import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("maxTemp")
sc = SparkContext(conf=conf)
# sc = SparkContext("local","RatingCounts")

def parseLine(line):
	fileds = line.split(',')
	stationID = fileds[0]
	entryType = fileds[2]
	temp = float(fileds[3])*0.1*(9.0/5.0)+32.0
	return (stationID, entryType, temp)


lines = sc.textFile("data/1800.csv")
rdd = lines.map(parseLine)
minTemps = rdd.filter(lambda x: 'TMAX' in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTempStation = stationTemps.reduceByKey(lambda x,y:max(x,y))

results = minTempStation.collect()
for result in results:
	print(result)