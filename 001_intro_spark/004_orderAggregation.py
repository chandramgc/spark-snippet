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

orderItemsFilter = orderItems.filter(lambda l : int(l.split(",")[1]) == 2)
orderItemsSubTotal = orderItemsFilter.map(lambda l : float(l.split(",")[4]))

consolePrint = orderItemsFilter
orderItemsTotal = orderItemsSubTotal.reduce(add)
orderItemsTotalL = orderItemsSubTotal.reduce(lambda x, y: x + y)
orderItemsFilterMin = orderItemsFilter.reduce(lambda x, y: 
		x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y
	)
orderItemsGroupByOrderId = orderItemsMap.groupByKey()
revenuePerOrderId = orderItemsGroupByOrderId. \
		map(lambda oi : (oi[0], sum(oi[1])))


print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

for i in consolePrint.take(50) : print(i) 
print("Order Items filter sum :" + str(orderItemsTotalL))
print("Order Items filter min :" + str(orderItemsFilterMin))
for i in revenuePerOrderId.take(10) : print(i)

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
