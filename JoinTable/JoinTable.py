import pyspark
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.context import SparkContext
from pyspark import SparkConf


conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.appName("Megre").\
    config("spark.some.config.option", "some-value").getOrCreate()

tweetDF = spark.read.json("/home/tong/data/tweets.json")
tweetDF.show()

lookUpTable = spark.read.json("/home/tong/data/cityStateMap.json")
lookUpTable.show()

# inner join table base on column city and geo, and drop same column
dfJoin = tweetDF.join(lookUpTable,lookUpTable.city == tweetDF.geo,"inner").drop(lookUpTable.city)

dfJoin.show()

# count the number of state for each state
countDF = dfJoin.groupBy("state").count()
countDF.show()
countDF.write.json("/home/tong/data/joinFile")




