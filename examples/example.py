from pyspark import SparkConf, SparkContext
conf=SparkConf().setMaster("local").setAppName("example")

sc = SparkContext(conf=conf)

rdd = sc.parallelize([1,2,3,4,5])
rdd = rdd.map(lambda x: x*x)
rdd = rdd.filter(lambda x: x%2==0)
results = rdd.collect()

for result in results:
    print(f"************{result}")
