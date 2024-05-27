from airflow import DAG # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
import pandas as pd # type: ignore

from datetime import datetime
import statistics as sts


dag = DAG('pythonOperator',
        description='pythonOperator DAG learning ',
        schedule_interval=None,
        start_date=datetime(2024,5,20), 
        catchup=False,
        tags=['python', 'demonstration', "Data Cleaning"])

def clean_dataset():
    dataset = pd.read_csv('/opt/airflow/data/Churn.csv', sep=';')
    dataset.columns = ['Id', 'Score', 'Estado', 'Genero', 'Idade', 'Patrimonio',
                       'Saldo', 'Produtos', 'TemCartCredito', 'Ativo', 'Salario', 'Saiu']
    
    median = sts.median(dataset['Salario'])
    dataset['Salario'].fillna(median, inplace=True)

    dataset['Genero'].fillna('Masculino', inplace=True)

    median = sts.median(dataset['Idade'])
    dataset.loc[(dataset['Idade']<0) | (dataset['Idade'] > 120), 'Idade'] = median

    dataset.drop_duplicates(subset="Id", keep='first', inplace=True)

    dataset.to_csv("/opt/airflow/data/Churn_Clean.csv", sep=';', index=False)


task1 = PythonOperator(task_id='clean_data', python_callable=clean_dataset, dag=dag)

task1