from airflow import DAG # type: ignore
from airflow.operators.bash_operator import BashOperator # type: ignore
from datetime import datetime

dag = DAG('DAG_demo_auxiliar',
        description='auxiliar DAG for demonstration purposes ',
        schedule_interval=None,
        start_date=datetime(2024,5,20), 
        catchup=False,
        tags=['auxiliar', 'demonstration'])

task1 = BashOperator(task_id='tsk1', bash_command='sleep 1', dag=dag)
task2 = BashOperator(task_id='tsk2', bash_command='sleep 1', dag=dag)

#Ordem de prescedencia

task1 >> task2