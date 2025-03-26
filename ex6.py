from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("Parquet_Processing").getOrCreate()

# Load Parquet file
df = spark.read.parquet("hdfs://namenode:9000/user/hadoop/employees_filtered.parquet")

# Group by department and calculate average salary
df_grouped = df.groupBy("department").avg("salary")

# Show results
df_grouped.show()

# Save as JSON
df_grouped.write.mode("overwrite").json("hdfs://namenode:9000/user/hadoop/output/avg_salary.json")

# Stop Spark
spark.stop()
