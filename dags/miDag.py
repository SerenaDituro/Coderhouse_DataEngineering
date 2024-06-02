from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ETL_funciones import extract_data, transform_data, load_data, check_and_alert, send_email_alert

# Definición de los argumentos del DAG
default_args = {
    'owner': 'Serena Dituro',
    'start_date': datetime(2024, 5, 1),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

# Definición del DAG
miDag = DAG(
    dag_id='DAG_ETL_FINANCES',
    default_args=default_args,
    description='''DAG diario que corre Tasks basados en un ETL con Python Operators dentro un Docker container. 
    Permite el mecanismo de Backfill y envía alertas por Email en caso de que el volumen actual de un ETL sea inferior 
    al volumen promedio (últimos 30 días)''',
    schedule_interval='@daily',  # Corre de forma diaria
    catchup=False
)

# Tareas:
# Extracción de datos
task_1 = PythonOperator(
    task_id='extraccion_datos',
    python_callable=extract_data,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=miDag,
)

# Transformación de datos
task_2 = PythonOperator(
    task_id='transformacion_datos',
    python_callable=transform_data,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=miDag,
)

# Carga de datos
task_3 = PythonOperator(
    task_id='carga_datos',
    python_callable=load_data,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=miDag,
)

task_4 = PythonOperator(
    task_id='envio_alerta_volumen',
    python_callable=check_and_alert,
    op_kwargs={'execution_date': '{{ execution_date }}'},
    provide_context=True,
    dag=miDag,
)

# Definición del orden de tareas
task_1 >> task_2
task_2 >> task_3
task_2 >> task_4
