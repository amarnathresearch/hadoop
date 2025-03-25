from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FilterExample").getOrCreate()

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6])

# Filter only even numbers
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)

print(filtered_rdd.collect())  # Output: [2, 4, 6]

spark.stop()
