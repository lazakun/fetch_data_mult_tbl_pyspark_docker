import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-42.7.4.jar"
sqlserver_driver_jar = "/usr/local/spark/resources/jars/mssql-jdbc-12.8.1.jre8.jar"

dag = DAG(
    dag_id = "fetch_data_test",
    default_args = {
        "owner": "airflow",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)
start = DummyOperator(task_id="start", dag=dag)
python_job = SparkSubmitOperator(
    task_id="spark_job",
    conn_id="spark_default",
    conf={"spark.master":spark_master},
    jars=f"{postgres_driver_jar},{sqlserver_driver_jar}",  # Include both drivers
    driver_class_path=f"{postgres_driver_jar}:{sqlserver_driver_jar}",  # Include both drive
    application="/usr/local/spark/app/fetch_data_test_app.py",
    dag=dag
)

end = DummyOperator(task_id="end", dag=dag)
start >> python_job >> end