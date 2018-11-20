import os, sys
import time
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

orderItemsMap = orderItems. \
		map(lambda oi: (int(oi.split(",")[1]), oi))

orders201312 = order. \
		filter(lambda o: o.split(",")[1][:7] == "2013-12"). \
		map(lambda o: (int(o.split(",")[0]),o))

orders201401 = order. \
                filter(lambda o: o.split(",")[1][:7] == "2014-01"). \
                map(lambda o: (int(o.split(",")[0]),o))

orders201312Join = orders201312.join(orderItemsMap)
orders201401Join = orders201401.join(orderItemsMap)

orderItems201312 = orders201312. \
		join(orderItemsMap). \
		map(lambda oi: oi[1][1])
orderItems201401 = orders201401. \
                join(orderItemsMap). \
                map(lambda oi: oi[1][1])

products201312 = orderItems201312. \
		map(lambda p: int(p.split(",")[2]))
products201401 = orderItems201401. \
                map(lambda p: int(p.split(",")[2]))

allproducts = products201312.union(products201401)
commonproducts = products201312.intersection(products201401)
product201312only = products201312.subtract(products201401)

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

print("products201312 : " + str(products201312.count()))
print("products201401 : " + str(products201401.count()))
print("allproducts : " + str(allproducts.count()))
print("allproductsDistinct : " + str(products201312.distinct().count()))
print("commonproducts : " + str(commonproducts.count()))
print("product201312only : " + str(product201312only.count()))

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
