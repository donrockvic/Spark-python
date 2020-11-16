import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf=conf)
# sc = SparkContext("local","RatingCounts")



lines = sc.textFile("data/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResult = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResult.items():
	print(str(key)+" : "+str(value))