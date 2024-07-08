# Q.2. Read ncdc data from mysql table and print average temperature per year in DESC order.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
            .appName('spark-Assign-que2') \
            .getOrCreate()

dbUrl = 'jdbc:mysql://localhost:3306/classwork_db'
dbDriver = 'com.mysql.cj.jdbc.Driver'
dbUser = 'root'
dbPasswd = 'manager'
dbTable = 'ncdcdata'

ncdcdata = spark.read\
    .option('url', dbUrl)\
    .option('driver', dbDriver)\
    .option('user', dbUser) \
    .option('password', dbPasswd) \
    .option('dbtable', dbTable) \
    .format('jdbc') \
    .load()

ncdcdata.createOrReplaceTempView("ncdcdata_view")

yravgtemps = spark.sql("SELECT yr, avg(temp) avgtemp FROM ncdcdata_view WHERE temp != 9999 AND quality IN (0,1,2,4,5,9) GROUP BY yr ORDER BY avgtemp DESC")
yravgtemps.printSchema()
yravgtemps.show()


spark.stop()









