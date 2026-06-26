from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

def on_failure(context):
    print(f"Task Failed: {context['task_instance'].task_id}")

def on_success(context):
    print(f"Task Success: {context['task_instance'].task_id}")

default_args = {
    'owner': 'faheem',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 5, 25),
    'depends_on_past': False,
    'on_failure_callback': on_failure,
    'on_success_callback': on_success,
}

dag = DAG(
    'ridesharing_pipeline',
    default_args=default_args,
    description='Ridesharing Medallion Pipeline',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['ridesharing', 'medallion', 'etl'],
    doc_md="This DAG runs the Ridesharing Medallion Pipeline Bronze -> Silver -> Validation -> Gold",
)

bronze_task = BashOperator(
    task_id='bronze_layer',
    bash_command='echo "Running Bronze Layer"',
    dag=dag,
    priority_weight=4,
    trigger_rule='all_success',
)

silver_task = BashOperator(
    task_id='silver_layer',
    bash_command='echo "Running Silver Layer"',
    dag=dag,
    priority_weight=3,
    trigger_rule='all_success',
)

validation_task = BashOperator(
    task_id='validation_layer',
    bash_command='echo "Running Validation"',
    dag=dag,
    priority_weight=2,
    trigger_rule='all_success',
)

gold_task = BashOperator(
    task_id='gold_layer',
    bash_command='echo "Running Gold Layer"',
    dag=dag,
    priority_weight=1,
    trigger_rule='all_success',
)

bronze_task >> silver_task >> validation_task >> gold_task