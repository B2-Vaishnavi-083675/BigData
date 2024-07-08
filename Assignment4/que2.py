# Q.2. Find max sal per dept per job in emp.csv le.

# Import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
            .appName("MaxSalPerDeptPerJob") \
            .getOrCreate()

# Load data and create DataFrame
filepath = '/home/vaishnavi/BigData/BigData/data/emp.csv'
emp_df = spark.read \
        .option('header', 'false') \
        .option('inferSchema', 'true') \
        .csv(filepath) \
        .toDF('empno', 'ename', 'job', 'mgr', 'hiredate', 'sal', 'comm', 'dept')

# Group by department and job, and calculate the maximum salary
result = emp_df \
        .groupBy('dept', 'job') \
        .max('sal') \
        .withColumnRenamed('max(sal)', 'maxsal')

# Display result
result.show()

# Stop Spark session
spark.stop()


