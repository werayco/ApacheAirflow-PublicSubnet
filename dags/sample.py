from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='mysampledag',
    default_args=default_args,
    description='A simple DAG with BashOperator',
    schedule='@daily',
    start_date=datetime(2025, 11, 23),
    catchup=False,
) as dag:

    print_date = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    list_files = BashOperator(
        task_id='list_tmp_files',
        bash_command='ls -la /tmp'
    )

    echo_message = BashOperator(
        task_id='echo_message',
        bash_command='echo "Hello from Airflow!"'
    )

    print_date >> list_files >> echo_message
