from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Step 1: Start Spark Session
spark = SparkSession.builder.appName("MLlibLogisticRegression").getOrCreate()

# Step 2: Create sample data
data = [
    (25, 50000, 0),
    (30, 60000, 0),
    (35, 65000, 1),
    (40, 70000, 1),
    (45, 80000, 1)
]

columns = ["Age", "Salary", "Purchased"]

df = spark.createDataFrame(data, columns)

df.show()

# Step 3: Assemble features into a single vector
assembler = VectorAssembler(inputCols=["Age", "Salary"], outputCol="features")
final_df = assembler.transform(df).select("features", "Purchased")
final_df.show()

# Step 4: Split data into training and test sets
train_data, test_data = final_df.randomSplit([0.8, 0.2], seed=42)

# Step 5: Initialize Logistic Regression
lr = LogisticRegression(labelCol="Purchased", featuresCol="features")

# Step 6: Train model
model = lr.fit(train_data)

# Step 7: Predict on test data
predictions = model.transform(test_data)
predictions.show()

# Step 8: Evaluate model
evaluator = BinaryClassificationEvaluator(labelCol="Purchased")
accuracy = evaluator.evaluate(predictions)

print(f"Model Accuracy: {accuracy:.2f}")

spark.stop()
