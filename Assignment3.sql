#Q.1. Execute following queries on MySQL emp database using Recursive CTEs (not supported in Hive 3.x).

#1. Find years in range 1975 to 1985, where no emps were hired.


WITH RECURSIVE years(n) AS (
    (SELECT 1975)
    UNION
    (SELECT n+1 FROM years WHERE n<1985)
)
SELECT * FROM years WHERE n NOT IN (SELECT YEAR(hire) FROM emp);

+------+
| n    |
+------+
| 1975 |
| 1976 |
| 1977 |
| 1978 |
| 1979 |
| 1984 |
| 1985 |
+------+


#2. Display emps with their level in emp hierarchy. Level employee is Level of his manager + 1.

WITH RECURSIVE eh AS(
    (SELECT empno,ename,mgr, 1 AS level FROM emp WHERE mgr IS NULL)
    UNION
    (SELECT e.empno,e.ename,e.mgr,level+1 FROM emp e INNER JOIN eh ON eh.empno = e.mgr)
)
SELECT * FROM eh;

+-------+--------+------+-------+
| empno | ename  | mgr  | level |
+-------+--------+------+-------+
|  7839 | KING   | NULL |     1 |
|  7566 | JONES  | 7839 |     2 |
|  7698 | BLAKE  | 7839 |     2 |
|  7782 | CLARK  | 7839 |     2 |
|  7499 | ALLEN  | 7698 |     3 |
|  7521 | WARD   | 7698 |     3 |
|  7654 | MARTIN | 7698 |     3 |
|  7788 | SCOTT  | 7566 |     3 |
|  7844 | TURNER | 7698 |     3 |
|  7900 | JAMES  | 7698 |     3 |
|  7902 | FORD   | 7566 |     3 |
|  7934 | MILLER | 7782 |     3 |
|  7369 | SMITH  | 7902 |     4 |
|  7876 | ADAMS  | 7788 |     4 |
+-------+--------+------+-------+


/*3. Create a "newemp" table with foreign constraints enabled for "mgr" column. Also enable DELETE ON CASCADE for the same. Insert data into the
table from emp table. Hint: You need to insert data levelwise to avoid FK constraint error.*/


CREATE TABLE newemp (
    empno INT PRIMARY KEY,
    ename VARCHAR(255),
    job VARCHAR(255),
    mgr INT,
    hire DATE,
    sal DECIMAL(10, 2),
    comm DECIMAL(10, 2),
    deptno INT,
    CONSTRAINT fk_mgr FOREIGN KEY (mgr) REFERENCES newemp(empno) ON DELETE CASCADE
);



INSERT INTO newemp (empno, ename, job, mgr, hire, sal, comm, deptno)
SELECT empno, ename, job, mgr, hire, sal, comm, deptno
FROM emp
WHERE mgr IS NULL;

INSERT INTO newemp (empno, ename, job, mgr, hire, sal, comm, deptno)
SELECT empno, ename, job, mgr, hire, sal, comm, deptno
FROM emp
WHERE mgr IS NOT NULL AND mgr IN (SELECT empno FROM newemp);



#4. From "newemp" table, delete employee KING. What is result?


DELETE FROM newemp WHERE ename = 'KING';

SELECT * FROM newemp;

-> Empty set (0.00 sec)



# Q.2. Implement movie recommendation in python/java + hive.

#!/usr/bin/python

from pyhive import hive

# hive config
host_name = 'localhost'
port = 10000
user = 'alkeshlajurkar'
password = ''
db_name = 'classwork'

# get hive connection
conn = hive.Connection(host=host_name, port=port, username=user, password=password, database=db_name, auth='CUSTOM')

# get the cursor object
cur = conn.cursor()

# execute the sql query using cursor
sal = input('Enter minimum salary: ')
sql = "SELECT * FROM emp_staging WHERE sal > " + str(sal)
cur.execute(sql)

# collect/process result
result = cur.fetchall()
for row in result:
    print(row)

# close the connection
conn.close()




# Q.3. Create ORC table emp_job_part to partition emp data jobwise. Upload emp data dynamically into these partitions.

CREATE TABLE emp_job_part(
empno INT,
ename STRING,
mgr INT,
hire DATE,
sal DOUBLE,
comm DOUBLE,
deptno INT 
)
PARTITIONED BY (job STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC;

DESCRIBE emp_job_part;

INSERT INTO emp_job_part
PARTITION(job)
SELECT empno, ename, mgr, hire, sal, comm, deptno, job FROM emp_staging;

SELECT * FROM emp_job_part
WHERE job='ANALYST';


+---------------------+---------------------+-------------------+--------------------+-------------------+--------------------+----------------------+-------------------+
| emp_job_part.empno  | emp_job_part.ename  | emp_job_part.mgr  | emp_job_part.hire  | emp_job_part.sal  | emp_job_part.comm  | emp_job_part.deptno  | emp_job_part.job  |
+---------------------+---------------------+-------------------+--------------------+-------------------+--------------------+----------------------+-------------------+
| 7788                | SCOTT               | 7566              | 1982-12-09         | 3000.0            | NULL               | 20                   | ANALYST           |
| 7902                | FORD                | 7566              | 1981-12-03         | 3000.0            | NULL               | 20                   | ANALYST           |
+---------------------+---------------------+-------------------+--------------------+-------------------+--------------------+----------------------+-------------------+



/* Q.4. Create ORC table emp_job_dept_part to partition emp data jobwise and deptwise. Also divide them into two buckets by empno. Upload emp data
dynamically into these partitions.*/

CREATE TABLE emp_job_dept_part(
empno INT, ename STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE  
)
PARTITIONED BY (job STRING, deptno INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC;

DESCRIBE emp_job_dept_part;

INSERT INTO emp_job_dept_part
PARTITION(job, deptno)
SELECT empno, ename, mgr, hire, sal, comm, job, deptno FROM emp_staging;

SELECT * FROM emp_job_dept_part
WHERE job='CLERK' AND deptno=10;

+--------------------------+--------------------------+------------------------+-------------------------+------------------------+-------------------------+------------------------+---------------------------+
| emp_job_dept_part.empno  | emp_job_dept_part.ename  | emp_job_dept_part.mgr  | emp_job_dept_part.hire  | emp_job_dept_part.sal  | emp_job_dept_part.comm  | emp_job_dept_part.job  | emp_job_dept_part.deptno  |
+--------------------------+--------------------------+------------------------+-------------------------+------------------------+-------------------------+------------------------+---------------------------+
| 7934                     | MILLER                   | 7782                   | 1982-01-23              | 1300.0                 | NULL                    | CLERK                  | 10                        |
+--------------------------+--------------------------+------------------------+-------------------------+------------------------+-------------------------+------------------------+---------------------------+



# Q.5. Load Fire data into Hive in a staging table "fire_staging".

CREATE TABLE fire_staging (
    Call_Number STRING,
    Unit_ID STRING,
    Incident_Number STRING,
    Call_Type STRING,
    Call_Date STRING,
    Watch_Date STRING,
    Received_DtTm STRING,
    Entry_DtTm STRING,
    Dispatch_DtTm STRING,
    Response_DtTm STRING,
    On_Scene_DtTm STRING,
    Transport_DtTm STRING,
    Hospital_DtTm STRING,
    Call_Final_Disposition STRING,
    Available_DtTm STRING,
    Address STRING,
    City STRING,
    Zipcode_of_Incident STRING,
    Battalion STRING,
    Station_Area STRING,
    Box STRING,
    Original_Priority STRING,
    Priority STRING,
    Final_Priority STRING,
    ALS_Unit BOOLEAN,
    Call_Type_Group STRING,
    Number_of_Alarms INT,
    Unit_Type STRING,
    Unit_sequence_in_call_dispatch INT,
    Fire_Prevention_District STRING,
    Supervisor_District STRING,
    Neighborhoods_Analysis_Boundaries STRING,
    RowID STRING,
    case_location STRING,
    data_as_of STRING,
    data_loaded_at STRING,
    Analysis_Neighborhoods STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES('skip.header.line.count'='1');

LOAD DATA LOCAL INPATH '/home/vaishnavi/BigData/data/Fire_Service_Calls_Sample.csv'
INTO TABLE fire_staging;



/* Q.6. Create a transactional ORC table "fire_data" with appropriate data types partitioned by city and buckted by call number into 4 buckets. Load data from
staging table into this table.*/


CREATE TABLE fire_data(
    Call_Number STRING, Unit_ID STRING, Incident_Number STRING, Call_Type STRING, Call_Date STRING, Watch_Date STRING,
    Received_DtTm STRING, Entry_DtTm STRING, Dispatch_DtTm STRING, Response_DtTm STRING, On_Scene_DtTm STRING,
    Transport_DtTm STRING, Hospital_DtTm STRING, Call_Final_Disposition STRING, Available_DtTm STRING, Address STRING,
    Zipcode_of_Incident STRING, Battalion STRING, Station_Area STRING, Box STRING, Original_Priority STRING,
    Priority STRING, Final_Priority STRING, ALS_Unit BOOLEAN, Call_Type_Group STRING, Number_of_Alarms INT,
    Unit_Type STRING, Unit_sequence_in_call_dispatch INT, Fire_Prevention_District STRING, Supervisor_District STRING,
    Neighborhoods_Analysis_Boundaries STRING, RowID STRING, case_location STRING, data_as_of STRING, data_loaded_at STRING,
    Analysis_Neighborhoods STRING
)
PARTITIONED BY (City STRING)
CLUSTERED BY (Call_Number) INTO 4 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES('transactional'='true');

INSERT INTO fire_data PARTITION(City)
SELECT * FROM fire_staging;



# Q.7. Execute following queries on fire dataset.

#1. How many distinct types of calls were made to the fire department?

SELECT COUNT(DISTINCT Call_Type) AS Distinct_Call_Types
FROM fire_data;

#2. What are distinct types of calls made to the fire department?

SELECT DISTINCT Call_Type
FROM fire_data;


#3. Find out all responses for delayed times greater than 5 mins?

SELECT *
FROM fire_data
WHERE (unix_timestamp(Response_DtTm, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(Received_DtTm, 'yyyy-MM-dd HH:mm:ss')) > 300;


#4. What were the most common call types?

SELECT Call_Type, COUNT(*) AS Call_Count
FROM fire_data
GROUP BY Call_Type
ORDER BY Call_Count DESC
LIMIT 10;



#5. What zip codes accounted for the most common calls?

SELECT Zipcode_of_Incident, Call_Type, COUNT(*) AS Call_Count
FROM fire_data
GROUP BY Zipcode_of_Incident, Call_Type
ORDER BY Call_Count DESC
LIMIT 10; 


#6. What San Francisco neighborhoods are in the zip codes 94102 and 94103?

SELECT DISTINCT Neighborhoods_Analysis_Boundaries
FROM fire_data
WHERE Zipcode_of_Incident IN ('94102', '94103');


#7. What was the sum of all calls, average, min, and max of the call response times?

SELECT 
    SUM(response_time) AS Sum_Response_Time,
    AVG(response_time) AS Avg_Response_Time,
    MIN(response_time) AS Min_Response_Time,
    MAX(response_time) AS Max_Response_Time
FROM (
    SELECT 
        (unix_timestamp(Response_DtTm, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(Received_DtTm, 'yyyy-MM-dd HH:mm:ss')) / 60 AS response_time
    FROM fire_data
) t;


#8. How many distinct years of data are in the CSV file?

SELECT COUNT(DISTINCT YEAR(Call_Date)) AS Distinct_Years
FROM fire_data;


#9. What week of the year in 2018 had the most re calls?

SELECT week_number, COUNT(*) AS num_calls
FROM (
    SELECT weekofyear(to_date(Call_Date)) AS week_number
    FROM fire_data
    WHERE year(to_date(Call_Date)) = 2018
) t
GROUP BY week_number
ORDER BY num_calls DESC
LIMIT 1;


#10. What neighborhoods in San Francisco had the worst response time in 2018?


SELECT 
    Neighborhoods_Analysis_Boundaries,
    AVG(unix_timestamp(Response_DtTm, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(Received_DtTm, 'yyyy-MM-dd HH:mm:ss')) / 60 AS avg_response_time
FROM fire_data
WHERE year(to_date(Received_DtTm)) = 2018
GROUP BY Neighborhoods_Analysis_Boundaries
ORDER BY avg_response_time DESC;

