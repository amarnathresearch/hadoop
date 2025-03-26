from pyspark import SparkConf, SparkContext

# Step 1: Create Spark Context (Local Mode)
conf = SparkConf().setAppName("BigDataWordCount").setMaster("local[*]")  # Use all available cores
sc = SparkContext(conf=conf)

# Step 2: Load large text file from Local File System
input_path = "file:///home/user/input/large_text.txt"  # Adjust to actual local file path
text_rdd = sc.textFile(input_path)

# Step 3: Perform word count
word_counts = (text_rdd
    .flatMap(lambda line: line.split(" "))  
    .map(lambda word: (word.lower().strip(".,!?()[]{}"), 1))  
    .filter(lambda x: x[0] != '')  
    .reduceByKey(lambda a, b: a + b)  
)

# Step 4: Save output to Local File System
output_path = "file:///home/user/output/wordcount_result"
word_counts.saveAsTextFile(output_path)

# Step 5: Stop Spark Context
sc.stop()
