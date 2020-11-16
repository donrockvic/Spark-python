import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')
from pyspark import SparkConf, SparkContext
import re
import collections

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf=conf)

def normalizeWords(text):
	return re.compile(r'\W+',re.UNICODE).split(text.lower())

lines = sc.textFile("data/book.txt")
words = lines.flatMap(normalizeWords)
# wordCounts= words.countByValue()

wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)

def reverse(x):
	return (x[1],x[0])

sortedResult = wordCounts.map(reverse).sortByKey()
results = sortedResult.collect()

for result in results:
	count = str(result[0])
	wordclean = result[1].encode('ascii','ignore')
	if wordclean:
		print(wordclean,count)