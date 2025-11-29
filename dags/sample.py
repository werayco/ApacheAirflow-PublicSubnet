from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonVirtualEnvOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable


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
    

@dag(
    dag_id='scrapefinance',
    default_args=default_args,
    description='A DAG that scrapes Yfinance data using PythonVirtualEnvOperator and saves to MongoDB',
    schedule='0 9 * * *',
    start_date=datetime(2025, 11, 23),
    catchup=False
)
def scrapeyfinancedata():
    def scrape_yfinance_save_to_mongo():
        import yfinance as yf
        import pymongo
        import os
        data = yf.download("AAPL")
        mongoconnection = Variable("MONGO")
        os.makedirs("/opt/downloads", exist_ok=True)
        csv_path = "/opt/downloads/data.csv"
        data.to_csv(csv_path)

        print(f"Data saved to {csv_path}")
        client = pymongo.MongoClient(mongoconnection)
        db = client["finance_data"]
        collection = db["AAPL"]
        records = data.reset_index().to_dict(orient="records")
        if records:
            collection.insert_many(records)
        print(f"Inserted {len(records)} records into MongoDB collection 'AAPL'.")

    scrape_task = PythonVirtualEnvOperator(
        task_id="scrape_yfinance_saveascsv_and_save_to_mongodb",
        python_callable=scrape_yfinance_save_to_mongo,
        requirements=["yfinance", "pymongo"], 
        system_site_packages=False,
        python_version="3.9",
    )

    send_email_task = EmailOperator(
        task_id='email_notification',
        to=['screenbondhq@gmail.com', 'heisrayco@gmail.com'],
        subject='The Yfinance data has been scraped and stored successfully',
        html_content='<h3>Your DAG has completed successfully!</h3>',
    )

    scrape_task >> send_email_task

# scrapefinance_dag = scrapeyfinancedata()
