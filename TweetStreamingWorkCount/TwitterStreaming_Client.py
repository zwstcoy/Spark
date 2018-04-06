from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    sc = SparkContext("local[2]", appName="test")
    sc.setLogLevel("ERROR")

    # Create streaming Context
    ssc = StreamingContext(sc, 1)

    # Get content though localhost and port 45670
    lines = ssc.socketTextStream("localhost", 45670)

    # Count words in lines
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)

    # Print key/value in each line
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
