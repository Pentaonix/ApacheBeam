from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

TEMPLATE_PATH = "gs://dataflow-apachebeam/template/batch_job_bq"

# Define the default_args dictionary with your desired settings.
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance using the `with` statement.
with models.DAG(
    'my_dataflow_dag',
    default_args=default_args,
    description='A DAG with DataflowOperator tasks',
    schedule_interval=timedelta(days=3),
    catchup=False,
) as dag:

    # Define Task 1: DataflowOperator to start a Dataflow Flex Template.
    task_dataflow_1 = DataflowTemplatedJobStartOperator(
        task_id='dataflow_task',
        template=TEMPLATE_PATH,
        project_id='dataflow-apachebeam-397608',
        location='asia-northeast1',  # Specify your preferred location.
    )

    # Define a PythonOperator task to print "DONE" when the Dataflow job is done.
    def print_done():
        print("DONE")

    task_python_done = PythonOperator(
        task_id='print_done',
        python_callable=print_done,
    )

    # Define the order in which tasks should run.
    task_dataflow_1 >> task_python_done
