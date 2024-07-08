# Q.3. Find deptwise total sal from emp.csv and dept.csv. Print dname and total sal.

#  Import packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

# Create SparkSession
spark = SparkSession.builder \
            .appName("DeptwiseTotalSal") \
            .getOrCreate()

# Load emp.csv data and create DataFrame without header
emp_filepath = '/home/vaishnavi/BigData/data/emp.csv'
emp_df = spark.read \
        .option('header', 'false') \
        .option('inferSchema', 'true') \
        .csv(emp_filepath) \
        .toDF('empno', 'ename', 'job', 'mgr', 'hiredate', 'sal', 'comm', 'dept')

# Load dept.csv data and create DataFrame without header
dept_filepath = '/home/alkeshlajurkar/DBDA_DATA/BigData/BigData/data/dept.csv'
dept_df = spark.read \
        .option('header', 'false') \
        .option('inferSchema', 'true') \
        .csv(dept_filepath) \
        .toDF('deptno', 'dname', 'loc')

# Calculate total salary per department in emp_df
deptwise_total_sal = emp_df \
    .groupBy('dept') \
    .sum('sal') \
    .withColumnRenamed('sum(sal)', 'total_sal')

# Join with dept_df to get department names
result = deptwise_total_sal.join(dept_df, deptwise_total_sal.dept == dept_df.deptno) \
    .select('dname', 'total_sal')

# Display result
result.show()

# Stop Spark session
spark.stop()
