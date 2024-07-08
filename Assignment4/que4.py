# Q.4. Count number of movie ratings per year.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName("MovieRatingsCount").getOrCreate()

# Define the file path containing your ratings data
filepath = '/home/vaishnavi/BigData/data/movies/ratings.csv'

# Load the ratings data from CSV file
ratings_df = spark.read.csv(filepath, header=True, inferSchema=True)

# Convert timestamp to a readable date format if needed
# ratings_df = ratings_df.withColumn("rtime", from_unixtime("timestamp"))

# Extract the year from the 'timestamp' column and count ratings per year
ratings_per_year = ratings_df.withColumn("year", year(from_unixtime("timestamp"))) \
                             .groupBy("year") \
                             .count() \
                             .orderBy("year")

# Show the result
ratings_per_year.show()

# Stop the SparkSession
spark.stop()
