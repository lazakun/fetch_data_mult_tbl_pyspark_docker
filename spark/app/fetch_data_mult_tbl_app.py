# Import necessary libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit

# Define the application name and master URL for Spark
appName = "PySpark SQL Server to PostGres- via JDBC"
master = "spark://spark:7077"

# Configure Spark with the application name and master URL
conf = SparkConf() \
    .setAppName(appName) \
    .setMaster(master) \

# Create or get an existing SparkContext with the given configuration
sc = SparkContext.getOrCreate(conf=conf)

# Create a SparkSession using the SparkContext
spark = SparkSession(sc)

# Define the target PostgreSQL database URL with connection details
target_url = "jdbc:postgresql://192.168.65.254:5432/chinook?user=postgres&password=Ieseg"

# Define the source SQL Server database URL with connection details
source_url = "jdbc:sqlserver://192.168.65.254:1433;databaseName=AdventureWorks2022;user=potey;password=lazlo;encrypt=true;trustServerCertificate=true;"

# SQL query to select specific table names from the SQL Server database
sql = """select  t.name as table_name from sys.tables t 
where t.name in ('Employee_Buyer','Employee_Janitor','Employee_Prod_Tech','Employee_Sales_Rep') """

# Read a script from a file (this script will be appended to the SQL queries)
file = open("/usr/local/spark/resources/script.txt", "r")
script = file.read()

# Read data from the SQL Server database using the SQL query
dfs = spark.read. \
    format("jdbc"). \
    options(url=source_url, query=sql). \
    load()

# Collect the data from the DataFrame into a list of rows
data_collect = dfs.collect()

# Loop through each row in the collected data and print the table name
for row in data_collect:
    print(row["table_name"])

# Get the last table name from the collected data
table_name_var = row["table_name"]

# Read the first row of the last table to create an empty DataFrame with the same schema
dest_df = spark.read \
    .format("jdbc") \
    .option("url", source_url) \
    .option("query", "select top 1 * from dbo." + table_name_var) \
    .load()

# Add a column to the DataFrame to store the table name
dest_df = dest_df.withColumn("table_name", lit(table_name_var))

# Limit the DataFrame to 0 rows to create an empty DataFrame with the same schema
dest_df = dest_df.limit(0)

# Loop through each row in the collected data again to process each table
for row in data_collect:
    table_name_var = row["table_name"]
    
    # Read the entire table data from the SQL Server database
    src_df = spark.read \
        .format("jdbc") \
        .option("url", source_url) \
        .option("query", "select * from dbo." + table_name_var + ' ' + script) \
        .load()
    
    # Add a column to the DataFrame to store the table name
    src_df = src_df.withColumn("table_name", lit(table_name_var))
    
    # Union the current table data with the destination DataFrame
    dest_df = dest_df.union(src_df)

# Write the combined DataFrame to the PostgreSQL database
dest_df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", target_url) \
    .option("dbtable", "employee_output_airflow") \
    .save()

# Stop the SparkSession to release resources
spark.stop()