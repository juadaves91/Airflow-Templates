from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

def display_variable():
    my_var = Variable.get("my_var")
    print('variable' + my_var)
    return my_var

def display_variable_env():
    my_var2 = Variable.get("my_var_enviroment")
    print('variable' + my_var2)
    return my_var2

dag = DAG(dag_id="variable_dag", start_date=datetime(2021, 1, 1),
    schedule_interval='@daily', catchup=False)

task = PythonOperator(task_id='display_variable', python_callable=display_variable, dag=dag)

task1 = PythonOperator(task_id='display_variable_env', python_callable=display_variable_env, dag=dag)

task >> task1