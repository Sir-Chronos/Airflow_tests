from airflow import DAG, Dataset # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
import pandas as pd # type: ignore

from datetime import datetime


dag = DAG('producer',
        description='producer DAG ',
        schedule_interval=None,
        start_date=datetime(2024,5,20), 
        catchup=False,
        tags=['python', 'demonstration', "Data Cleaning"])

mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=';')
    dataset.to_csv("/opt/airflow/data/Churn_new.csv", sep=';')

task1 = PythonOperator(task_id='Producer', python_callable=my_file, dag=dag, outlets=[mydataset])

task1