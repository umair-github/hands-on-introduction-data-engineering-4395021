'''Load DAG in airflow/dags'''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

with DAG(
        dag_id='load_dag',
        description='Load Airflow DAG',
        schedule_interval=None,
        start_date=datetime(2023, 12, 6),
        catchup=False) as dag:
        
        load_task = BashOperator(
            task_id='load_task',
            bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-transform-data.csv\
                          top_level_domains" | \
                          sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-load-db.db',
            dag=dag)