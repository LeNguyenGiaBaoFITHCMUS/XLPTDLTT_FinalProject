from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "student",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="creditcard_daily_pipeline_exec",
    default_args=default_args,
    description="Daily orchestration: aggregate -> export for Power BI (docker exec)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 23 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["creditcard", "daily", "airflow"],
) as dag:

    daily_aggregate = BashOperator(
        task_id="daily_aggregate",
        bash_command="""
        set -e
        docker exec spark-master spark-submit /opt/spark/apps/daily_aggregate.py
        """,
    )

    export_powerbi = BashOperator(
        task_id="export_powerbi",
        bash_command="""
        set -e
        docker exec spark-master spark-submit /opt/spark/apps/export_powerbi.py
        """,
    )

    verify_output = BashOperator(
        task_id="verify_hdfs_output",
        bash_command="""
        set -e
        docker exec namenode hdfs dfs -ls /powerbi || true
        """,
    )

    download_to_local = BashOperator(
        task_id="download_to_local",
        bash_command="""
        set -e
        cd /opt/airflow
        python3 /opt/spark/apps/download_powerbi.py
        echo "✅ Đã download dữ liệu vào ./powerbi_data/"
        """,
    )

    daily_aggregate >> export_powerbi >> verify_output >> download_to_local