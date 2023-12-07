'''Extract DAG'''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

with DAG(
        schedule_interval=None,
        start_date=datetime(2023, 12, 5),
        catchup=False) as dag:

    task1 = BashOperator(
            task_id='extract_taks',
            bash_command='wget -c https://pkgstore.datahub.io/core/top-level-domain-names/top-level-domain-names.csv_csv/data/667f4464088f3ca10522e0e2e39c8ae4/top-level-domain-names.csv_csv.csv -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-extract-data.csv'
            )