from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "mci_group_8",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="orders_api_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description="Micro-batch Orders API -> Data Lake -> Spark -> ClickHouse",
    tags=["orders", "spark", "clickhouse"],
) as dag:

    fetch_orders = BashOperator(
        task_id="fetch_orders",
        bash_command="python /opt/airflow/dags/scripts/fetch_orders_stream.py",
    )

    process_and_load = BashOperator(
        task_id="process_and_load",
        bash_command="python /opt/airflow/dags/scripts/process_orders_spark.py",
    )

    fetch_orders >> process_and_load
