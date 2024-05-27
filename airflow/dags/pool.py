from airflow import DAG # type: ignore
from airflow.operators.bash_operator import BashOperator # type: ignore

from datetime import datetime

dag = DAG('Pool',
        description='pool DAG learning ',
        schedule_interval=None,
        start_date=datetime(2024,5,20), 
        catchup=False,
        tags=['pool', 'demonstration'])

# Data ingestion
task1 = BashOperator(task_id='tsk1', bash_command='sleep 2', dag=dag, pool='demonstration_pool', priority_weight=15)
task2 = BashOperator(task_id='tsk2', bash_command='sleep 2', dag=dag, pool='demonstration_pool', priority_weight=5)
task3 = BashOperator(task_id='tsk3', bash_command='sleep 2', dag=dag, pool='demonstration_pool', priority_weight=1)
task4 = BashOperator(task_id='tsk4', bash_command='sleep 2', dag=dag, pool='demonstration_pool', priority_weight=10)
