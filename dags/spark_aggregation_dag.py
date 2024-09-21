from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

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

run_weekly_aggregation = BashOperator(
    task_id="run_weekly_aggregation",
    bash_command="spark-submit --master local /opt/airflow/dags/weekly_aggregation.py"
                 " --execution_date {{ ds }}"
                 " --input_path /opt/airflow/input"
                 " --output_path /opt/airflow/output"
                 " --daily_path /opt/airflow/daily",
    dag=dag,
)
