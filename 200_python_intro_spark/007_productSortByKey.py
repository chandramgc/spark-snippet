import os, sys
import time
from pyspark import SparkConf, SparkContext, SQLContext
from operator import add
startTimeStr = time.strftime("%c")
fileName = os.path.basename(sys.argv[0]).split('.')[0]
conf = SparkConf().setAppName(fileName).setMaster("yarn-client").set("spark.ui.port","12888")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
#sc.setLogLevel("WARN")

# Code Starting

products = sc.textFile("/public/retail_db/products")

productMap = products. \
		filter(lambda p: p.split(",")[4] != ""). \
                map(lambda l : (float(l.split(",")[4]), l))
productMap1 = products. \
                filter(lambda po: po.split(",")[4] != ""). \
                map(lambda lo : ((int(lo.split(",")[1]), float(lo.split(",")[4])), lo))

productSortedByPrice = productMap.sortByKey()

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

print("productMap :")
for i in productMap.take(5) : print(i)
print("productSortedByPrice :")
for i in productSortedByPrice.take(10) : print(i)
print("productSortedByKey :")
for i in productMap1.sortByKey(False).take(25): print(i)

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
