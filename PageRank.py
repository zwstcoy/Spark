import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Load the adjacency list file
#1 2
#2 3 4
#3 4
#4 1 5
#5 3

AdjList1 = sc.textFile("--/02AdjacencyList.txt")

#read the file and split by space
AdjList2 = AdjList1.map(lambda line : line.split(" "))

# create list with first element in adjList2 as first element in adjList2, ex: [(1,[2]), (2,[3,4])...]
AdjList3 = AdjList2.map(lambda x:(int(x[0]),[int(y) for y in x[1:]]))

#save in memory
AdjList3.persist()

# count the total node in file, node = 5
nNumOfNodes = AdjList3.count()

# Initialize each page's rank with .2
PageRankValues = AdjList3.mapValues(lambda v : .2)

# Run 30 iterations of PageRank
print ("Run 30 Iterations")
for i in range(1, 31):
    print("Number of Iterations","\n",i)

    #combin the node list with pagerank value, ex: [(1, ([2],.2)) , (2, [3, 4], .2)...]
    JoinRDD = AdjList3.join(PageRankValues)

    #calculate the degree for each node [(2, .2/1),(3, .2/2),(4, .2/2)...]
    contributions = JoinRDD.flatMap(lambda (x, (y, z)) : [(v,z/len(y)) for v in y])

    #combin the value of each node
    accumulations = contributions.reduceByKey(lambda x, y : x+y)

    #calcuate the final pagerank value with c=.85 and set as new pagerank value for next iteration
    PageRankValues = accumulations.mapValues(lambda v : v*.85+.15/float(nNumOfNodes))

    print("PageRankValues")
    print(PageRankValues.collect())

print ("=== Final PageRankValues ===")
print (PageRankValues.collect())

# save the final result
PageRankValues.coalesce(1).saveAsTextFile("--/PageRankValues_Final")
