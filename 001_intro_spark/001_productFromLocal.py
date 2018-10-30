import os, sys
import time
from pyspark import SparkConf, SparkContext
startTimeStr = time.strftime("%c")
fileName = os.path.basename(sys.argv[0]).split('.')[0]
conf = SparkConf().setAppName(fileName).setMaster("yarn-client").set("spark.ui.port","12888")
sc = SparkContext(conf=conf)
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
#sc.setLogLevel("WARN")

# Code Starting

productCollection = open("/data/retail_db/products/part-00000")
productRaw = sc.parallelize(productCollection)
consolePrint = productRaw.take(1)

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

print consolePrint

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
