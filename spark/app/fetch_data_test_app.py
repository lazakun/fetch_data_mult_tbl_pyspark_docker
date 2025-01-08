from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit

appName = "PySpark SQL Server to PostGres Test - via JDBC"
master = "spark://spark:7077"
conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \
    .set("spark.driver.extraClassPath","/home/jovyan/work/jars/*") \
    .set("spark.executor.extraClassPath","/home/jovyan/work/jars/*") \

sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession(sc)

spark

target_url = "jdbc:postgresql://192.168.65.254:5432/chinook?user=postgres&password=Ieseg"

source_url = "jdbc:sqlserver://192.168.65.254:1433;databaseName=AdventureWorks2022;user=potey;password=lazlo;encrypt=true;trustServerCertificate=true;"

df=spark.read. \
    format("jdbc"). \
    options(url=source_url, dbtable="person.person_test"). \
    load()

df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", target_url) \
    .option("dbtable", "employee_output_airflow_test") \
    .save()

spark.stop()