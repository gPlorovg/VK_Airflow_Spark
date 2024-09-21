from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_weekly_aggregation_dag",
    default_args=default_args,
    description="DAG для подсчёта количества CRUD действий пользователей за неделю",
    schedule_interval="0 7 * * *",
    start_date=days_ago(1),
    catchup=False,
)

run_weekly_aggregation = SparkSubmitOperator(
    task_id='run_weekly_aggregation',
    application='weekly_aggregation.py',
    conn_id='spark_default',
    verbose=True,
    application_args=[
        "--execution_date", "{{ ds }}",
        "--input_path", "./input/",
        "--output_path", "./output/",
        "--daily_path", "./daily/",
    ],
    trigger_rule="all_done",
    dag=dag,
)
