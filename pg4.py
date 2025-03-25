from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PeopleExample").getOrCreate()

# 1. Load CSV
df = spark.read.csv("people.csv", header=True, inferSchema=True)

# 2. Print schema & show data
df.printSchema()
df.show()

# 3. Select Name and City
df.select("Name", "City").show()

# 4. Filter Age > 30
df.filter(df.Age > 30).show()

# 5. Add new column
df.withColumn("AgeAfter5Years", df.Age + 5).show()

# 6. Group by City and count
df.groupBy("City").count().show()

# 7. Sort by Age descending
df.orderBy(df.Age.desc()).show()

# 8. SQL Query
df.createOrReplaceTempView("people")
spark.sql("SELECT * FROM people WHERE City = 'Chicago'").show()

spark.stop()
