from airflow import DAG # type: ignore
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
from airflow.operators.python_operator import BranchPythonOperator # type: ignore

from datetime import datetime
import random

def random_number():
    return random.randint(1, 100)

def even_or_odd(**context):
    n = context['task_instance'].xcom_pull(task_ids='generate_random_number')
    return 'even_task' if n % 2 == 0 else 'odd_task'
 

dag = DAG('Branch',
        description='brench DAG learning ',
        schedule_interval=None,
        start_date=datetime(2024,5,20), 
        catchup=False,
        tags=['branch', 'demonstration'])

# Data ingestion
RNG = PythonOperator(task_id='generate_random_number', python_callable=random_number, dag=dag)
branch = BranchPythonOperator(task_id='branch_path', python_callable=even_or_odd, provide_context=True, dag=dag)

even_task = BashOperator(task_id='even_task', bash_command='echo "even" ', dag=dag)
odd_task = BashOperator(task_id='odd_task', bash_command='echo "odd" ', dag=dag)

RNG >> branch
branch >> even_task
branch >> odd_task