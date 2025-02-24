'''Basic ETL DAG in airflow/dags'''
from datetime import datetime, date
import pandas as pd
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    dag_id='etl_dag',
    description='ETL Airflow DAG',
    schedule_interval=None,
    start_date=datetime(2023, 12, 6),
    catchup=False) as dag:

    extract_task = BashOperator(
        task_id='extract_taks',
        bash_command='wget -c https://pkgstore.datahub.io/core/top-level-domain-names/top-level-domain-names.csv_csv/data/667f4464088f3ca10522e0e2e39c8ae4/top-level-domain-names.csv_csv.csv \
                     -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-extract-data.csv'
        )
       
    def transform_data():
        """Read in the file, and write a transformed file out"""
        today = date.today()
        df = pd. read_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-extract-data.csv')
        generic_type_df = df[df['Type'] == 'generic']
        generic_type_df['Date'] = today.strftime('%Y-%m-%d')
        generic_type_df.to_csv('/workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-transform-data.csv', index=False)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        dag=dag)
    
    load_task = BashOperator(
        task_id='load_task',
        bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-transform-data.csv\
        top_level_domains" | \
        sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-load-db.db',
        dag=dag)

extract_task >> transform_task >> load_task

