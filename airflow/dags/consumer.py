from airflow import DAG, Dataset # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
import pandas as pd # type: ignore

from datetime import datetime

mydataset = Dataset("/opt/airflow/data/Churn_new.csv")

dag = DAG('consumer',
        description='consumer DAG ',
        schedule=[mydataset],
        start_date=datetime(2024,5,20), 
        catchup=False,
        tags=['python', 'demonstration', "Data Cleaning"])


def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn_new.csv", sep=';')
    dataset.to_csv("/opt/airflow/data/Churn_new2.csv", sep=';')

task1 = PythonOperator(task_id='Producer', python_callable=my_file, dag=dag, provide_context=True)

task1