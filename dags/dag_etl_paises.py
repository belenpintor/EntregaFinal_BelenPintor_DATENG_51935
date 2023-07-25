from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from script_etl import create_table, load_data, transform_data, get_data, create_connection

QUERY_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS bapintor_coderhouse.ciudades (
    city VARCHAR PRIMARY KEY,
    "Housing" DECIMAL(10, 2),
    "Cost of Living" DECIMAL(10, 2),
    "Startups" DECIMAL(10, 2),
    "Venture Capital" DECIMAL(10, 2),
    "Travel Connectivity" DECIMAL(10, 2),
    "Commute" DECIMAL(10, 2),
    "Business Freedom" DECIMAL(10, 2),
    "Safety" DECIMAL(10, 2),
    "Healthcare" DECIMAL(10, 2),
    "Education" DECIMAL(10, 2),
    "Environmental Quality" DECIMAL(10, 2),
    "Economy" DECIMAL(10, 2),
    "Taxation" DECIMAL(10, 2),
    "Internet Access" DECIMAL(10, 2),
    "Leisure & Culture" DECIMAL(10, 2),
    "Tolerance" DECIMAL(10, 2),
    "Outdoors" DECIMAL(10, 2),
    "process_date" TIMESTAMP DISTKEY
);
"""


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
    
    task1 = PostgresOperator(
        task_id='crear_tabla_',
        sql=QUERY_CREATE_TABLE, 
        postgres_conn_id='redshift_belen',
        autocommit=True
    )

    task2 = PythonOperator(
        task_id='transformar_datos',
        python_callable=transform_data,
        op_kwargs={'data': get_data()},
        provide_context=True  # Habilita el contexto de Airflow para acceder a XComs
    )
    

    task3 = PythonOperator(
        task_id='cargar_datos',
        python_callable=load_data,
        provide_context=True 
    )

    task1 >> task2 >> task3
