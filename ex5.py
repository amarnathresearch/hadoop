from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("JSON_Processing").getOrCreate()

# Load JSON file
df = spark.read.json("hdfs://namenode:9000/user/hadoop/people.json")

# Create SQL table
df.createOrReplaceTempView("people")

# Query people from New York
df_ny = spark.sql("SELECT name, age FROM people WHERE city = 'New York'")
df_ny.show()

# Save results as CSV
df_ny.write.mode("overwrite").csv("hdfs://namenode:9000/user/hadoop/output/people_ny.csv")

# Stop Spark
spark.stop()
