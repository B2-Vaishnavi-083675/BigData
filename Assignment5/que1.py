# Q.1. Clean NCDC data and write year, temperature and quality data into mysql table.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
            .appName('spark-Assign-que1') \
            .getOrCreate()


ncdcFile = '/home/vaishnavi/BigData/data/ncdc'
ncdc = spark.read\
        .text(ncdcFile)
ncdc.printSchema()
# ncdc.show(truncate=False)

regex = r'^.{15}([0-9]{4}).{68}([-\+][0-9]{4})([0-9]).*$'
ncdcdata = ncdc\
         .select(
            regexp_extract('value', regex, 1).cast('int').alias('yr'), 
             regexp_extract('value', regex, 2).cast('int').alias('temp'),
             regexp_extract('value', regex, 3).cast('int').alias('quality')
         )
ncdcdata.printSchema()
ncdcdata.show()

dbUrl = 'jdbc:mysql://localhost:3306/classwork_db'
dbDriver = 'com.mysql.cj.jdbc.Driver'
dbUser = 'root'
dbPasswd = 'manager'
dbTable = 'ncdcdata'
ncdcdata.write\
    .option('url', dbUrl)\
    .option('driver', dbDriver)\
    .option('user', dbUser) \
    .option('password', dbPasswd) \
    .option('dbtable', dbTable) \
    .mode('OVERWRITE') \
    .format('jdbc') \
    .save()

spark.stop()
print('Exitting...')
