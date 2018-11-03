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

order = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")

orderItemsFilter = orderItems.filter(lambda l : int(l.split(",")[1]) == 2)
orderItemsSubTotal = orderItemsFilter.map(lambda l : float(l.split(",")[4]))

orderTotal = orderItemsSubTotal.reduce(add)

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

#for i in consolePrint.take(50) : print(i) 
print(orderTotal)

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
