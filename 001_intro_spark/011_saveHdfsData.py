import os, sys
import time
from operator import add
from pyspark import SparkConf, SparkContext, SQLContext
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

orderMap = order. \
		map(lambda l : (int(l.split(",")[0]) , l.split(",")[1]))
orderItemsMap = orderItems. \
                map(lambda li : (int(li.split(",")[1]) , float(li.split(",")[4])))

revenuePerOrderId = orderItemsMap. \
		reduceByKey(add). \
		map(lambda r: str(r[0]) + "\t" + str(r[1]))

revenuePerOrderId.saveAsTextFile("/user/chandramgc/data/revenue_per_order_id")

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

for i in revenuePerOrderId.take(10) : print(i) 

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
