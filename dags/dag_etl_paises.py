from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from airflow.operators.email_operator import EmailOperator
from email import message
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import smtplib
import json
from script_etl import  transform_data, get_data, enviar_success, enviar_alerta


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
    "process_date" DATETIME DISTKEY
);
"""

def verificar_threshold(**context):
    postgres_hook = PostgresHook(postgres_conn_id='redshift_belen')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    # Leer la configuración de los umbrales desde el archivo JSON
    with open("config_threshold.json", 'r') as json_config:
        config = json.load(json_config)

    for city, categories in config['thresholds'].items():
        # Construir la consulta dinámicamente usando las columnas de la tabla
        columns_query_str = ', '.join(f'"{category}"' for category in categories)
        query = f"""
            SELECT {columns_query_str}
            FROM bapintor_coderhouse.ciudades
            WHERE city = %s
        """

        cursor.execute(query, (city,))
        row = cursor.fetchone()

        if not row:
            print(f"No se encontraron datos para la ciudad: {city}")
            continue

        for i, categoria in enumerate(row):
            categoria_nombre = list(categories.keys())[i]
            thresholds = categories[categoria_nombre]
            min_t = thresholds.get('min')
            max_t = thresholds.get('max')

            if min_t is not None and categoria < min_t:
                enviar_alerta(city, categoria_nombre, categoria, min_t, max_t, is_under_threshold=True)

            if max_t is not None and categoria > max_t:
                enviar_alerta(city, categoria_nombre, categoria, min_t, max_t, is_under_threshold=False)

    connection.close()

SMTP_HOST = Variable.get("SMTP_HOST")
SMTP_PORT = Variable.get("SMTP_PORT")
SMTP_EMAIL_FROM= Variable.get("SMTP_EMAIL_FROM")
SMTP_PASSWORD= Variable.get("SMTP_PASSWORD")
SMTP_EMAIL_TO= Variable.get("SMTP_EMAIL_TO")

default_args = {
    'owner': 'Belenpintor',
    "start_date": datetime(2023, 7, 21),
    "retries": 1,
    'email_on_failure': True,
    'email_on_retry': True,
    'max_active_runs':1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    default_args=default_args,
    dag_id='dag_etl_paises',
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
        task_id='obtener_datos',
        python_callable=get_data,
        provide_context=True
    )

    task3 = PythonOperator(
        task_id='transformar_datos',
        python_callable=transform_data,
        provide_context=True,
        op_args=[task2.output] 
        
    )
    
    
    task_alerta = PythonOperator(
        task_id='verificar_threshold',
        python_callable=verificar_threshold,
        provide_context=True,
    )
    
    task_succes=PythonOperator(
        task_id='dag_envio_success',
        python_callable=enviar_success,
        trigger_rule='all_success'
    )


task1>>  task2>>  task3>> task_alerta>>task_succes
