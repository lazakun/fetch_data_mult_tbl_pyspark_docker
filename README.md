Project Tasks:

query multiple large similar (same schema) tables residing in a MSSQL server database

the query logic would be shared as an SQL script in a text format

the final output of the query should identify the tables where the data resides

the final output of the query should be copied (overwrite option) to PostgreSQL DB

airflow should be used to orchestrate and schedule the pipeline

use apache spark's master-worker architecture 

implement in a docker environment

![airflow_spark_docker](https://github.com/user-attachments/assets/6498af8c-3f35-4335-8239-9712267f95ac)


This project contains the following containers:

* postgres: Postgres database for Airflow metadata and a Test database to test whatever you want.
    * Image: postgres:9.6
    * Database Port: 5432

* airflow-webserver: Airflow webserver and Scheduler.
    * Image: docker-airflow-spark:1.10.7_3.1.2
    * Port: 8282

* spark: Spark Master.
    * Image: bitnami/spark:3.1.2
    * Port: 8181

* spark-worker-N: Spark workers. You can add workers copying the containers and changing the container name inside the docker-compose.yml file.
    * Image: bitnami/spark:3.1.2

* jupyter-spark: Jupyter notebook with pyspark for interactive development.
  * Image: jupyter/pyspark-notebook:spark-3.1.2
  * Port: 8888

### Check if you can access

Airflow: http://localhost:8282

Spark Master: http://localhost:8181

PostgreSql - Database Test:

* Server: localhost:5432
* Database: test
* User: test
* Password: postgres

Postgres - Database airflow:

* Server: localhost:5432
* Database: airflow
* User: airflow
* Password: airflow

Jupyter Notebook: http://127.0.0.1:8888
  * For Jupyter notebook, you must copy the URL with the token generated when the container is started and paste in your browser. The URL with the token can be taken from container logs using:
  
        $ docker logs -f docker-jupyter-spark-1
