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

order_json = sqlContext.read.json("/public/retail_db_json/orders/")
order_json_copy = sqlContext.read.format("json").load("/public/retail_db_json/orders/")
order = sc.textFile("/public/retail_db/orders")
orderfilter = order.filter(lambda l : l.split(",")[3] in ("CLOSED","COMPLETE"))
consolePrint = orderfilter

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

for i in consolePrint.take(10) : print(i) 

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
