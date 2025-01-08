# Import necessary Airflow modules
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# Define Spark master URL and paths to JDBC driver JAR files for PostgreSQL and SQL Server
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-42.7.4.jar"
sqlserver_driver_jar = "/usr/local/spark/resources/jars/mssql-jdbc-12.8.1.jre8.jar"

# Define the DAG (Directed Acyclic Graph) for the Airflow workflow
dag = DAG(
    dag_id="fetch_data_multiple",  # Unique identifier for the DAG
    default_args={
        "owner": "airflow",  # Owner of the DAG
        "start_date": airflow.utils.dates.days_ago(1)  # Start date for the DAG (1 day ago)
    },
    schedule_interval="@daily"  # Schedule the DAG to run daily
)

# Define the start task using PythonOperator to print a message
start = PythonOperator(
    task_id="start",  # Unique identifier for the task
    python_callable=lambda: print("Jobs started"),  # Python function to execute
    dag=dag  # Associate the task with the DAG
)

# Define the Spark job task using SparkSubmitOperator to submit a Spark application
python_job = SparkSubmitOperator(
    task_id="spark_job",  # Unique identifier for the task
    conn_id="spark_default",  # Airflow connection ID for Spark
    conf={"spark.master": spark_master},  # Spark configuration (master URL)
    jars=f"{postgres_driver_jar},{sqlserver_driver_jar}",  # JAR files for JDBC drivers
    driver_class_path=f"{postgres_driver_jar}:{sqlserver_driver_jar}",  # Classpath for drivers
    application="/usr/local/spark/app/fetch_data_mult_tbl_app.py",  # Path to the Spark application script
    dag=dag  # Associate the task with the DAG
)

# Define the end task using PythonOperator to print a message
end = PythonOperator(
    task_id="end",  # Unique identifier for the task
    python_callable=lambda: print("Jobs completed successfully"),  # Python function to execute
    dag=dag  # Associate the task with the DAG
)

# Define the task dependencies: start -> python_job -> end
start >> python_job >> end