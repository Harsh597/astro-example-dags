from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
)

# Define tasks

# Dummy task to start the workflow
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Task to print a message
def print_hello():
    print("Hello from the PythonOperator!")

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Dummy task to signify the end of the workflow
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define the task dependencies
start_task >> hello_task >> end_task
