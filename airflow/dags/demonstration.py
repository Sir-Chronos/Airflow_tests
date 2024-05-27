#Importação do Airflow
from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.dummy_operator import DummyOperator # type: ignore
from airflow.operators.email_operator import EmailOperator # type: ignore
from airflow.operators.python_operator import PythonOperator #type: ignore
from airflow.operators.dagrun_operator import TriggerDagRunOperator # type: ignore
from airflow.models import Variable # type: ignore

from datetime import datetime, timedelta

def print_variable(**kwarg):
    my_var = Variable.get('demonstration')
    print(f"Demonstration value : {my_var}")

def task_write(**kwarg):
    kwarg['ti'].xcom_push(key='Xcom_value', value=80809090)

def task_read(**kwarg):
    value = kwarg['ti'].xcom_pull(key='Xcom_value')
    print(f'value: {value}')

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,5,21),
    'email': ['sirius.webdeveloper@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('DAG_demo',
        description='DAG created for learning purposes',
        schedule_interval='@daily',
        start_date=datetime(2024,5,22), 
        catchup=False,
        default_view='graph',
        tags=['Parallel', 'Email', 'Dummy', 'Xcom', 'Python', 'demonstration'])


# Task groups
tsk_group = TaskGroup('Data_ingestion', dag=dag)
tsk_group2 = TaskGroup('Data_processing', dag=dag)
tsk_group3 = TaskGroup('Log_relay', dag=dag)
tsk_group4 = TaskGroup('Data_storage', dag=dag)

# Data ingestion
task1 = BashOperator(task_id='tsk1', bash_command='sleep 1', dag=dag, task_group=tsk_group)
task2 = BashOperator(task_id='tsk2', bash_command='sleep 1', dag=dag, task_group=tsk_group)

# Dummy relay
taskDummy = DummyOperator(task_id='tskdummy', dag=dag)

# Data Processing
task3 = PythonOperator(task_id='tsk3', python_callable=task_write, dag=dag, task_group=tsk_group2)
task4 = PythonOperator(task_id='tsk4', python_callable=print_variable, dag=dag, task_group=tsk_group2)
task5 = TriggerDagRunOperator(task_id='tsk5', trigger_dag_id='DAG_demo_auxiliar', dag=dag, task_group=tsk_group2)

# Data Storage
task6 = PythonOperator(task_id='tsk6', python_callable=task_read, dag=dag, trigger_rule='none_failed', task_group=tsk_group4)

# Log Alerts
task7 = EmailOperator(task_id='Error_Alert',
                      to='sirius.webdeveloper@gmail.com',
                      subject='airflow error alert',
                      html_content="""
                      <h3>Error was detected !!</h3>
                      <p>Dag: Error_Alert</p>
                                   """,
                      dag=dag, trigger_rule='one_failed',
                      task_group=tsk_group3)

task8 = EmailOperator(task_id='Success_Alert',
                      to='sirius.webdeveloper@gmail.com',
                      subject='airflow success alert',
                      html_content="""
                      <h3>flow ocurred sucessfully</h3>
                      <p>Dag: Success_Alert</p>
                                   """,
                      dag=dag, trigger_rule='none_failed',
                      task_group=tsk_group3)


#Ordem de prescedencia
[task1, task2] >> taskDummy >> [task3, task4] >> task5
task5 >> [task6, task7, task8]