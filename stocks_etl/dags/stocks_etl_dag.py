from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os, sys

from stocks_etl.producers.reddit_producer import stream_to_kafka
from stocks_etl.pipelines.create_cassandra_tables_pipeline import create_cassandra_tables_pipeline 
from stocks_etl.pipelines.zip_modules import zip_modules_pipeline
from stocks_etl.producers.reddit_producer import create_kafka_topic_register_schema

# 5 retries for the tasks
default_args = {"owner": "rf", 
                "retries": 5,
                "retry_delta": timedelta(minutes=2)
                }

with DAG(
    dag_id="stocks_etl",
    default_args=default_args,
    start_date=datetime(2023, 12, 28),
    schedule_interval="@daily",
    catchup=False,
    tags=["reddit", "producer", "kafka"]
    ) as dag:

    # Create topic and Register Schema if thye do not exist
    create_register_kafka_topic = PythonOperator(
        task_id="create_register_kafka_topic",
        python_callable=create_kafka_topic_register_schema,
        op_kwargs={
            "topic_name": "reddit_posts"
        },
    )

    # Create cassandra tables
    create_cassandra_tables = PythonOperator(
        task_id="create_cassandra_tables",
        python_callable=create_cassandra_tables_pipeline
    )

    # Zip python modules to be used by spark-submit
    zip_python_modules = PythonOperator(
        task_id="zip_python_modules",
        python_callable=zip_modules_pipeline
    )

    # Extraction from reddit and produce to kafka
    reddit_produce = PythonOperator(
        task_id="reddit_produce_to_kafka",
        python_callable=stream_to_kafka,
        op_kwargs={
            "subreddit": "wallstreetbets",
            "time_filter": "day",
            "limit": None,
            "topic": "reddit_posts"
        },
    )

    # Spark reddit pipeline
    reddit_pipeline = SparkSubmitOperator(
        task_id = "reddit_pipeline",
        conn_id = "spark_default",
        application = "./dags/stocks_etl/pipelines/reddit_pipeline.py",
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.18,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector-driver_2.12:3.4.1",
        py_files = "./dags/stocks_etl/modules.zip",
    )

    # Spark stocks data pipeline
    stocks_data_pipeline = SparkSubmitOperator(
        task_id = "stocks_data_pipeline",
        conn_id = "spark_default",
        application = "./dags/stocks_etl/pipelines/stocks_data_pipeline.py",
        packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.18",
        py_files = "./dags/stocks_etl/modules.zip",
    )

    [create_register_kafka_topic, create_cassandra_tables, zip_python_modules] >> reddit_produce >> reddit_pipeline >> stocks_data_pipeline
