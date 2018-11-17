import os, sys
import time, itertools
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
                
productsGroupByCategoryId = productMap1.groupByKey()

t = productsGroupByCategoryId. \
		filter(lambda p: p[0] == 59).first()
l = sorted(t[1], key=lambda k: float(k.split(",")[4]), reverse=True)
l_map = map(lambda p: float(p.split(",")[4]), l)
topNPrices = sorted(set(l_map), reverse=True)[:3]
topNPricedProducts = itertools.takewhile(lambda p: float(p.split(",")[4]) in topNPrices, l)
lt = list(topNPricedProducts)

def getTopNPricedProductsPerCategoryId(productsPerCategoryId, topN):
	productsSorted = sorted(productsPerCategoryId[1],
		key=lambda k: float(k.split(",")[4]),
		reverse=True
		)
	productPrices = map(lambda p: float(p.split(",")[4]), productsSorted)
	topNPrices = sorted(set(productPrices), reverse=True)[:topN]
	return itertools.takewhile(lambda p:
			float(p.split(",")[4]) in topNPrices,
			productsSorted
			)	
topNPricedProducts = productsGroupByCategoryId. \
	flatMap(lambda p: getTopNPricedProductsPerCategoryId(p, 3

print ""
print "************************************************************************************************"
print "Job started at " + startTimeStr
print ""

print("productTop3PricedProducts:")
for i in lt : print(i)

print ""
print "Job stoped at " + time.strftime("%c")
print "************************************************************************************************"
print ""

# Code Ending
