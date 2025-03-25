from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
sc = spark.sparkContext

# Load text file into RDD
rdd = sc.textFile("file.txt")

# Split lines into words
words = rdd.flatMap(lambda line: line.split(" "))

# Map each word to (word, 1)
word_pairs = words.map(lambda word: (word, 1))

# Reduce by key to get word counts
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Collect and print result
output = word_counts.collect()
print(output)

spark.stop()
