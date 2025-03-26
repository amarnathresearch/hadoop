from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_extract, explode, when

# Initialize Spark session
spark = SparkSession.builder.appName("TwitterAnalysis").getOrCreate()

# Load the JSON dataset
tweets_df = spark.read.json("hdfs://namenode:9000/user/hadoop/tweets.json")

# Extract hashtags using regex
tweets_df = tweets_df.withColumn("hashtags", regexp_extract(col("text"), r"#(\w+)", 1))

# Extract sentiment based on keywords
tweets_df = tweets_df.withColumn("sentiment",
    when(lower(col("text")).rlike("love|amazing|happy"), "Positive")
    .when(lower(col("text")).rlike("sad|frustrating|fail"), "Negative")
    .otherwise("Neutral")
)

# Count hashtags
hashtag_counts = tweets_df.groupBy("hashtags").count().orderBy(col("count").desc())

# Count sentiment distribution
sentiment_counts = tweets_df.groupBy("sentiment").count()

# Show results
print("Top Hashtags:")
hashtag_counts.show()

print("Sentiment Distribution:")
sentiment_counts.show()

# Save results to HDFS
hashtag_counts.write.csv("hdfs://namenode:9000/user/hadoop/output/hashtag_counts")
sentiment_counts.write.csv("hdfs://namenode:9000/user/hadoop/output/sentiment_counts")

# Stop Spark session
spark.stop()
