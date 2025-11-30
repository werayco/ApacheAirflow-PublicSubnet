from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
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
) as dagsample:

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
    description='A DAG that scrapes yfinance data and saves to MongoDB',
    schedule='0 9 * * *',
    start_date=datetime(2025, 11, 23),
    catchup=False
)
def scrapeyfinancedata():

    def scrape_yfinance_save_to_mongo():
        import yfinance as yf
        import pymongo
        import os
        from airflow.models import Variable

        data = yf.download("AAPL")

        data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]    
        data = data.reset_index()    
        data['Date'] = data['Date'].astype(str)

        mongo_conn = Variable.get("MONGO")

        os.makedirs("/opt/airflow/downloads", exist_ok=True)
        csv_path = "/opt/airflow/downloads/data.csv"
        data.to_csv(csv_path, index=False)
        client = pymongo.MongoClient(mongo_conn)
        db = client["finance_data"]
        collection = db["AAPL"]
        records = data.reset_index().to_dict(orient="records")

        if records:
            print(records)
            collection.insert_many(records)

        print(f"Saved CSV + inserted {len(records)} records to MongoDB")
        
    def clearAAPLcollection():
        import pymongo
        from airflow.models import Variable

        mongo_conn = Variable.get("MONGO")
        client = pymongo.MongoClient(mongo_conn)

        db = client["finance_data"]
        collection = db["AAPL"]
        collection.delete_many({})
        print("Cleared AAPL collection in MongoDB")
    
    clear_db = PythonOperator(
        task_id="clear_aapl_collection",
        python_callable=clearAAPLcollection,
    )

    scrape_task = PythonOperator(
        task_id="scrape_yfinance_saveascsv_and_save_to_mongodb",
        python_callable=scrape_yfinance_save_to_mongo,
    )

    send_email_task = EmailOperator(
        task_id='email_notification',
        to=['screenbondhq@gmail.com', 'heisrayco@gmail.com'],
        subject='Yfinance data scraped and stored',
        html_content='<h3>Your DAG has completed successfully!</h3>',
    )


    clear_db >> scrape_task >> send_email_task


scrapefinance_dag = scrapeyfinancedata()


# scrapefinance_dag = scrapeyfinancedata()