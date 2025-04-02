from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner' : 'usuario',
    'depends_on_past' : False,
    'start_date' : datetime(2025, 4, 2),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retries_delay' : timedelta(minutes=1)
}

with DAG(
    dag_id = 'ETL-Diario',
    default_args = default_args,
    description = 'ETL diario ejecutado desde runner externo',
    schedule_interval = '59 23 * * *',
    catchup = False,
    tags = ['etl', 'pyspark']
) as dag:
    
    ejecutar_etl = BashOperator(
        task_id = 'ejecutar_etl',
        bash_command = 'python /opt/airflow/dags/etl_runner.py'
    )