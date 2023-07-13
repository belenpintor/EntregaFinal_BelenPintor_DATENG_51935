from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from script_etl import create_table, load_data, transform_data, get_data, create_connection

default_args = {
    'owner': 'Belenpintor',
    "start_date": datetime(2023, 7, 9),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    default_args=default_args,
    dag_id='dag_con_conexion_postgres',
    description='Entrega 3 Belén Pintor',
    start_date=datetime(2023, 7, 8),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PythonOperator(
        task_id='crear_tabla',
        python_callable=create_table
    )

    task2 = PythonOperator(
        task_id='transformar_datos',
        python_callable=transform_data,
        op_kwargs={'data': get_data()}
    )

    task3 = PythonOperator(
        task_id='cargar_datos',
        python_callable=load_data
    )

    task1 >> task2 >> task3
