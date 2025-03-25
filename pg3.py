from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ReduceExample").getOrCreate()

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Sum all elements using reduce()
sum_result = rdd.reduce(lambda a, b: a + b)

print(sum_result)  # Output: 15

spark.stop()
