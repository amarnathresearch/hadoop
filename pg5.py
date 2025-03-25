from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Step 1: Start SparkSession
spark = SparkSession.builder.appName("FileStreamingExample").getOrCreate()

# Step 2: Read streaming data from folder
lines = spark.readStream.format("text").load("input/")

# Step 3: Split lines into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Step 4: Count words
wordCounts = words.groupBy("word").count()

# Step 5: Output result to console
query = wordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

print("=== Waiting for new files in 'input_folder/'... ===")

# Step 6: Keep streaming until manually stopped
query.awaitTermination()
