from pyspark import SparkConf, SparkContext

# Step 1: Initialize Spark Context in Local Mode
conf = SparkConf().setAppName("BigDataLogAnalysis").setMaster("local[*]")  # Uses all CPU cores
sc = SparkContext(conf=conf)

# Step 2: Load large log file from HDFS
log_rdd = sc.textFile("hdfs://localhost:9000/user/hadoop/logs/access.log")

# Step 3: Extract HTTP status codes
status_counts = (log_rdd
    .map(lambda line: line.split(" ")[8] if len(line.split(" ")) > 8 else None)  # Extract HTTP status
    .filter(lambda code: code is not None)  # Remove invalid entries
    .map(lambda code: (code, 1))  # Map each status code to 1
    .reduceByKey(lambda a, b: a + b)  # Aggregate counts
)

# Step 4: Save results to HDFS
status_counts.saveAsTextFile("hdfs://localhost:9000/user/hadoop/output/log_analysis")

# Step 5: Stop Spark Context
sc.stop()
