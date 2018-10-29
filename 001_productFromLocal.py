from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Girish Spark Job").setMaster("yarn-client").set("spark.ui.port","12888")
sc = SparkContext(conf=conf)

# Code Starting

productCollection = open("/data/retail_db/products/part-00000")
productRaw = sc.parallelize(productCollection)
print productRaw.take(10)

# Code Ending
