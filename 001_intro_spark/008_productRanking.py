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
		filter(lambda p: p.split(",")[4] != "")
productMap1 = products. \
                filter(lambda po: po.split(",")[4] != ""). \
                map(lambda lo : (int(lo.split(",")[1]), lo))
                
topProduct = productMap.top(10, key=lambda k: float(k.split(",")[4])) # desc order
takeOrderProduct = productMap.takeOrdered(10, key=lambda k: float(k.split(",")[4])) # asc order

productGroupByCategoryId = productMap1.groupByKey()

topNProductsByCategory = productGroupByCategoryId. \
		flatMap(lambda p: sorted(p[1], key=lambda k: float(k.split(",")[4]),
		reverse=True)[:3])

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

print("productMap :")
for i in productMap.take(5) : print(i)
print("productSortedByPrice By top:")
for i in topProduct : print(i)
print("productSortedByPrice By takeOrder:")
for i in takeOrderProduct : print(i)
print("productSortedByCategory:")
for i in topNProductsByCategory.take(10) : print(i)

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
