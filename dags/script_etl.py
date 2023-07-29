import requests
import psycopg2
import pandas as pd
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator, PostgresHook
from email import message
from datetime import datetime, timedelta
import smtplib

config_thresholds= {
    "Tokyo": {
        "Environmental Quality": {"min": 2, "max": 8},
        "Safety": {"min": 1, "max": 8}
    },
    "Belgium": {
        "Environmental Quality": {"min": 3, "max": 7},
        "Safety": {"min": 2, "max": 8}
    }
}

insert_query_with_columns = """
    INSERT INTO bapintor_coderhouse.ciudades (
        city,
        "Business Freedom",
        "Commute",
        "Cost of Living",
        "Economy",
        "Education",
        "Environmental Quality",
        "Healthcare",
        "Housing",
        "Internet Access",
        "Leisure & Culture",
        "Outdoors",
        "Safety",
        "Startups",
        "Taxation",
        "Tolerance",
        "Travel Connectivity",
        "Venture Capital",
        "process_date")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    
"""


def get_data():
    url = "https://api.teleport.org/api/urban_areas/"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        ciudades = []
        for ciudad in data['_links']['ua:item']:
            ciudad_url = ciudad['href'] + "scores/"
            ciudad_nombre = ciudad["name"]

            response = requests.get(ciudad_url)
            if response.status_code == 200:
                ciudad_data = response.json()
                ciudades.append({ciudad_nombre:ciudad_data})
               
            else:
                print("Error al realizar la solicitud. Código de estado:", response.status_code)

        return ciudades
    else:
        print("Error al realizar la solicitud. Código de estado:", response.status_code)

def transform_data(ciudades_data):
    scores = []
    for dupla in ciudades_data:
        for ciudad_nombre, ciudad in dupla.items():
            for categoria in ciudad['categories']:
                nombre = categoria["name"]
                score = categoria['score_out_of_10']
                scores.append({"ciudad_nombre": ciudad_nombre, "nombre": nombre, "score": score})

    # Crear el DataFrame a partir de la lista scores
    df = pd.DataFrame(scores)
    df_pivot = df.pivot_table(index='ciudad_nombre', columns='nombre', values='score', fill_value=None)

    print("asi el dataframe", df_pivot)

    postgres_hook = PostgresHook(postgres_conn_id='redshift_belen')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    

    for index, row in df_pivot.iterrows():
        values = list(row.values)
        process_date = datetime.utcnow()  # Obtiene la fecha y hora actual para cada fila
        cursor.execute(insert_query_with_columns, [index] + values + [process_date])

        try:
            connection.commit()
            print("listo comit")
        except Exception as e:
            connection.rollback()
            print("Error:", str(e))

    connection.close()

def verificar_threshold(**context):
    postgres_hook = PostgresHook(postgres_conn_id='redshift_belen')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    for city, categories in config_thresholds.items():
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

def enviar_alerta(ciudad, categoria, valor, min_t, max_t, is_under_threshold):
    try:
        x = smtplib.SMTP(Variable.get("SMTP_HOST"), Variable.get("SMTP_PORT"))
        x.starttls()
        x.login(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_PASSWORD'))

        subject = f'ALERTA DE UMBRAL - {ciudad}'
        body_text = (
            f"El valor de la categoría '{categoria}' en la ciudad '{ciudad}' es {valor}, "
            f"que es {'menor' if is_under_threshold else 'mayor'} que el umbral "
            f"{'mínimo' if is_under_threshold else 'máximo'} ({min_t if is_under_threshold else max_t})."
        )
        message = f'Subject: {subject}\n\n{body_text}'
        message = message.encode('utf-8')
        x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'), message)

        print('Exito')
    except Exception as exception:
        print("Fallo: ",exception)
        raise exception


def enviar_success():
    try:
        x=smtplib.SMTP(Variable.get("SMTP_HOST"),Variable.get("SMTP_PORT"))
        x.starttls()#
        x.login(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_PASSWORD'))
        subject='SUCCES DAG'
        body_text="Mensaje sobre exito"
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail(Variable.get('SMTP_EMAIL_FROM'), Variable.get('SMTP_EMAIL_TO'),message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')
        raise exception