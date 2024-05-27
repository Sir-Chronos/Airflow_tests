# What's airflow ?

**Airflow** can be considered an orquestrator, meaning that it will manage the pipeline flow.
**Airflow** 

# Basic airflow operators

    from airflow import DAG # type: ignore
    from airflow.operators.bash_operator import BashOperator # type: ignore
    from airflow.operators.python_operator import PythonOperator #type: ignore
    from airflow.operators.dummy_operator import DummyOperator # type: ignore
    from airflow.operators.email_operator import EmailOperator # type: ignore
    from airflow.utils.task_group import TaskGroup # type: ignore

# 