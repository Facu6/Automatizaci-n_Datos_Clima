from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import subprocess


# Configuración del DAG
default_args = {
    'owner' : 'usuario',
    'depends_on_past' : False,
    'start_date' : datetime(2025, 3, 11),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
    'ETL-Diario',
    default_args = default_args,
    description = 'DAG para ejecutar ETL en 3 fases: Extracción, Procesamiento, Carga.',
    schedule_interval = '59 23 * * *',
    catchup = False
)

# Definir tareas del ETL en Airflow
@task(dag=dag)
def extraer_datos():
    subprocess.run(['python3', '/opt/airflow/dags/script.py', 'extraer'], check = True)
    print('Extracción completada.')

@task(dag=dag)
def procesar_datos():
    subprocess.run(['python3', '/opt/airflow/dags/script.py', 'procesar'], check = True)
    print('Procesamiento completado.')

@task(dag=dag)
def cargar_datos():
    subprocess.run(['python3', '/opt/airflow/dags/script.py', 'cargar'], check = True)
    print('Carga completada.')
    

# Definir flujo de ejecución
extraer = extraer_datos()
procesar = procesar_datos()
cargar = cargar_datos()

extraer >> procesar >> cargar


# INSTALAR TODAS LAS DEPENDENCIAS DENTRO DEL CONTENEDOR 