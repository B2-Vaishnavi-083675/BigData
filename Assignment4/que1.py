# Q.1. Wordcount using Spark Dataframes and nd top 10 words (except stopwords).

# Import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, desc

# Initialize Spark session
spark = SparkSession.builder \
            .appName("WordCount") \
            .getOrCreate()

# Filepath to the text file
filepath = '/home/vaishnavi/apache-hive-3.1.3-bin/LICENSE'

# Read the text file into a DataFrame
wordfile = spark.read.text(filepath)

# Test DataFrame
wordfile.printSchema()
wordfile.show(n=50,truncate=False)

# Perform operations on DataFrames
result = wordfile \
    .select(explode(split(lower(col("value")), '[^a-z0-9]+')).alias("word")) \
    .filter(~col("word").isin(
        '', 'the', 'a', 'an', 'or', 'of', 'and', 'to', 'any', 'and', 'for', 'in', 'by', 'from', 'that', 
        'under', 'over', 'is','with','are','be','on','you','this', 'such', 'as', 'shall', 'your', 'my'
    )) \
    .groupBy('word') \
    .count() \
    .orderBy(desc('count'))

# Display result
result.printSchema()
result.show(20)

