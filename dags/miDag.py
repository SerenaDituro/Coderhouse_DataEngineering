from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ETL_funciones import extract_data, transform_data, load_data

# Definición de los argumentos del DAG
default_args = {
    'owner': 'Serena Dituro',
    'start_date': datetime(2024, 1, 1),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

# Definición el DAG
miDag = DAG(
    dag_id='DAG_ETL_FINANCES',
    default_args=default_args,
    description='DAG diario que corre Tasks basados en un ETL con Python Operators dentro un Docker container',
    schedule_interval='@daily',  # Corre de forma diaria
    catchup=False
)

# Tareas:
# Extracción de datos
task_1 = PythonOperator(
    task_id='extraccion_datos',
    python_callable=extract_data,
    #op_args=["{{ ds }}"],
    provide_context=True,
    dag=miDag,
)

# Transformación de datos
task_2 = PythonOperator(
    task_id='transformacion_datos',
    python_callable=transform_data,
    provide_context=True,
    dag=miDag,
)

# Carga de datos
task_3 = PythonOperator(
    task_id='carga_datos',
    python_callable=load_data,
    provide_context=True,
    dag=miDag,
)

# Definición del orden de tareas
task_1 >> task_2 >> task_3