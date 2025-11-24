from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    "email_on_failure": True,
    "email": ["screenbondhq@gmail.com", "heisrayco@gmail.com"],
    "email_on_retry": True,
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
    send_email = EmailOperator(
        task_id='send_email',
        to=['screenbondhq@gmail.com', 'heisrayco@gmail.com'],
        subject='Airflow DAG Notification',
        html_content='<h3>Your DAG has completed successfully!</h3>',
    )


    print_date >> list_files >> send_email
