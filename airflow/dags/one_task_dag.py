'''One Task DAG'''
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow import DAG

default_args = {
        'owner': 'Umair',
        'depends_on_past': False,
        'email_on_failuer': False,
        'email_onretry': False,
        'retries': 0,
        'catchup': False,
        'start_date': datetime(2023, 11, 21)
}

with DAG(
        dag_id='one_task_dag',
        description='A one task Airflow DAG',
        schedule_interval=None,
        default_args=default_args
    ) as dag:

    task1 = BashOperator(
            task_id='one_taks',
            bash_command='echo "hello, linkedin learning!" > /workspaces/hands-on-introduction-data-engineering-4395021/lab/temp/create-this-file.txt',
            dag=dag)