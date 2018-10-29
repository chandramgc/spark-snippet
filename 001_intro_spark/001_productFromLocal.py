from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Girish Spark Job").setMaster("yarn-client").set("spark.ui.port","12888")
sc = SparkContext(conf=conf)
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
#sc.setLogLevel("WARN")

# Code Starting

productCollection = open("/data/retail_db/products/part-00000")
productRaw = sc.parallelize(productCollection)
print ""
print "************************************************************************************************"
print ""

print productRaw.take(1)

print ""
print "************************************************************************************************"
print ""

# Code Ending
