from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='simple_dag_example',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Define a dummy start task
    start = DummyOperator(
        task_id='start'
    )

    # Define a Python task that prints the current date
    def print_date():
        print(f"Current date is: {datetime.now()}")

    print_date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date
    )

    # Define a dummy end task
    end = DummyOperator(
        task_id='end'
    )

    # Set up the task dependencies
    start >> print_date_task >> end

