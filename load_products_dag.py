from datetime import datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from operator_s3_to_postgres import S3ToPostgresTransfer
from airflow.operators.python_operator import PythonOperator


def print_welcome():
    return 'Welcome from custom operator - Airflow DAG!'

dag = DAG('dag_insert_data', 
          description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

welcome_operator = PythonOperator(task_id='hello_task', 
                                python_callable=print_welcome, 
                                dag=dag)

process_dag = S3ToPostgresTransfer(
    task_id = 'dag_s3_to_postgres',
    schema =  'bootcampdb', #'public'
    table= 'products',
    s3_bucket = 's3-data-bootcamp',
    s3_key =  'products.csv',
    aws_conn_postgres_id = 'postgres_default',
    aws_conn_id = 'aws_default',   
    dag = dag
)

welcome_operator >> process_dag