from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta


# Función de notificación de fallos
def notificar_fallo(context):
    '''
    Envía una alerta por correo electrónico cuando alguna tarea del DAG falla.

    Argumentos:
    - context (dict): Proporcionado automáticamente por Airflow, contiene información 
      del contexto de la tarea que falló, como el ID del DAG, ID de la tarea, 
      fecha de ejecución y URL de los logs.
    '''
    
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    
    subject = f'[ALERTA] Fallo en el DAG: {dag_id} | Tarea: {task_id}'
    html_content = f"""
    <h3>Se ha producido un fallo en el DAG de Airflow</h3>
    <ul>
        <li><strong>DAG:</strong> {dag_id}</li>
        <li><strong>Tarea:</strong> {task_id}</li>
        <li><strong>Fecha de ejecución:</strong> {execution_date}</li>
        <li><strong>Logs:</strong> <a href= "{log_url}">Ver Logs</a></li>
    </ul>
    """
    send_email(
        to=['ejemplo_mail_falla@gmail.com'], # Acá debés colocar el email al cual se enviará la alerta en caso de falla
        subject=subject,
        html_content=html_content
    )

# Configuración del DAG
default_args = {
    'owner' : 'usuario',
    'depends_on_past' : False,
    'start_date' : datetime(2025, 4, 2),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retries_delay' : timedelta(minutes=1)
}

# Deinición del DAG
with DAG(
    dag_id = 'ETL-Diario',
    default_args = default_args,
    description = 'ETL diario ejecutado desde runner externo',
    schedule_interval = '59 23 * * *',
    catchup = False,
    tags = ['etl', 'pyspark'],
    on_failure_callback = notificar_fallo
) as dag:
    
    # Tarea 1 - Extracción de Datos
    extraccion = BashOperator(
        task_id = 'extraer_datos',
        bash_command = 'python /opt/airflow/dags/etl_runner.py extraer'
    )
    
    # Tarea 2 - Procesamiento de Datos
    procesamiento = BashOperator(
        task_id = 'procesar_datos',
        bash_command = 'python /opt/airflow/dags/etl_runner.py procesar'
    )
    
    # Tarea 3 - Carga de Datos
    carga = BashOperator(
        task_id = 'cargar_datos',
        bash_command = 'python /opt/airflow/dags/etl_runner.py cargar'
    )
    
    # Definimos plan de ejecución
    extraccion >> procesamiento >> carga