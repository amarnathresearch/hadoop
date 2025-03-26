from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("CSV_Processing").getOrCreate()

# Load CSV file
df = spark.read.option("header", "true").csv("hdfs://namenode:9000/user/hadoop/employees.csv")

# Convert salary to integer and filter employees with salary > 60000
df = df.withColumn("salary", df["salary"].cast("int"))
df_filtered = df.filter(df["salary"] > 60000)

# Show filtered data
df_filtered.show()

# Save results as Parquet
df_filtered.write.mode("overwrite").parquet("hdfs://namenode:9000/user/hadoop/output/employees_filtered.parquet")

# Stop Spark
spark.stop()
