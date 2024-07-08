 # Q.3 Count number of movie ratings per month using sql query (using temp views).



from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
            .appName('spark-Assign-que3') \
            .getOrCreate()


ratingsFile = '/home/vaishnavi/BigData/data/movies/ratings.csv'

ratings = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ratingsFile)

ratings.printSchema()
ratings.show()



dbUrl = 'jdbc:mysql://localhost:3306/classwork_db'
dbDriver = 'com.mysql.cj.jdbc.Driver'
dbUser = 'root'
dbPasswd = 'manager'
dbTable = 'ratingsdata'

ratings.write \
    .format('jdbc') \
    .option('url', dbUrl) \
    .option('driver', dbDriver) \
    .option('user', dbUser) \
    .option('password', dbPasswd) \
    .option('dbtable', dbTable) \
    .mode('overwrite') \
    .save()

print('Data Append Successfully...')

fetchratingdata = spark.read \
    .format('jdbc') \
    .option('url', dbUrl) \
    .option('driver', dbDriver) \
    .option('user', dbUser) \
    .option('password', dbPasswd) \
    .option('dbtable', dbTable) \
    .load()

fetchratingdata.createOrReplaceTempView("ratings_view")

monthrating = spark.sql("SELECT movieId, MONTH(from_unixtime(timestamp)) AS month, COUNT(rating) AS ratingcount FROM ratings_view GROUP BY movieId, month ORDER BY month")

monthrating.printSchema()
monthrating.show()

spark.stop()




