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

orderMap = order. \
                map(lambda l : (int(l.split(",")[0]) , l.split(",")[1]))
orderItemsMap = orderItems. \
                map(lambda li : (int(li.split(",")[1]) , float(li.split(",")[4])))
orderItemsMap2 = orderItems. \
		map(lambda l : (int(l.split(",")[1]), l))

revenuePerOrderId = orderItemsMap. \
		reduceByKey(lambda x, y: x + y)
revenuePerOrderIdAdd = orderItemsMap. \
                reduceByKey(add)
minSubtotalPerOrderId = orderItemsMap. \
		reduceByKey(lambda x, y: x if(x < y) else y)
minSubtotalPerOrderId2 = orderItemsMap2. \
                reduceByKey(lambda x, y: x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y)

#orderItemsSortedBySubtotalPerOrder = orderItemsGroupByOrderId2. \
#		map(lambda oi: sorted(oi[1], key=lambda k: float(k.split(",")[4]), reverse=True))
#orderItemsSortedBySubtotalPerOrderFlat = orderItemsGroupByOrderId2. \
#                flatMap(lambda oi: sorted(oi[1], key=lambda k: float(k.split(",")[4]), reverse=True))

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

for i in revenuePerOrderId.take(5) : print(i)
for i in minSubtotalPerOrderId.take(5) : print(i)
for i in minSubtotalPerOrderId2.take(5) : print(i)

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
