import findspark
findspark.init('/home/vicky/spark-3.0.0-bin-hadoop2.7')

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
import codecs

def LoadMoviesNames():
    """
    docstring
    """
    movieNames = {}
    with codecs.open("data/ml-100k/u.item","r", encoding='ISO-8859-1', errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Creating the Spark Session
spark = SparkSession.builder.appName("PopularMovieNames").getOrCreate()

nameDict = spark.sparkContext.broadcast(LoadMoviesNames())

schema = StructType([StructField("userID",IntegerType(),True), StructField("movieID",IntegerType(),True),StructField("rating",FloatType(), True),StructField("timeStamp",LongType(), True)])

lines = spark.read.option("sep","\t").schema(schema).csv("data/ml-100k/u.data")
lines.printSchema()

data = lines.groupBy("movieID").count()

def movieName(movieID):
    return nameDict.value[movieID]

lookupNameUDF = func.udf(movieName)

moviesWithName = data.withColumn("movieTitle",lookupNameUDF(func.col("movieID")))

sortedMovieName = moviesWithName.orderBy(func.desc("count"))

sortedMovieName.show(10, False)

spark.stop()