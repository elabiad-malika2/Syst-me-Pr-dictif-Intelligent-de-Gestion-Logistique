from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='spark_streaming_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    # Task 1: ensure services up (in docker-compose you'd have them up)
    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting pipeline..."'
    )

    # Task 2: run spark job (via spark-submit inside spark container)
    run_spark = BashOperator(
        task_id='run_spark_job',
        bash_command='docker exec -i $(docker ps -q -f name=spark) /opt/bitnami/spark/bin/spark-submit --master local[2] /app/streaming_job.py',
    )

    start >> run_spark
