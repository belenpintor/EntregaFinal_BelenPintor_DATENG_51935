import os
import requests
import psycopg2
import logging
from airflow.models import Variable
from os import environ as env




def create_connection():
    # Cargo variables especificadas en airflow

    print("creando conexion")
    db_user = Variable.get("DB_USER")
    db_password = Variable.get("DB_PASSWORD")
    db_host = Variable.get("DB_HOST")
    db_port = Variable.get("DB_PORT")
    db_database = Variable.get("DB_DATABASE")
    logging.info(f"DB_USER probando usuario 2: {db_user}")
    print("usuario probando 2",db_user)

    # Crear conexión a la base de datos
    connection = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_database
    )
    if connection:
        print("Conexión exitosa a la base de datos")
    else:
        print("Error al conectar a la base de datos")
    return connection


def transform_data(data):
    transformed_data = []
    for ciudad in data['_links']['ua:item']:
        ciudad_url = ciudad['href'] + "scores/"
        ciudad_nombre = ciudad["name"]
        scores = []

        response = requests.get(ciudad_url)
        if response.status_code == 200:
            ciudad_data = response.json()
            for categoria in ciudad_data['categories']:
                nombre = categoria["name"]
                score = categoria['score_out_of_10']
                scores.append({"nombre": nombre, "score": score})

            transformed_data.append({"ciudad": ciudad_nombre, "puntajes": scores})
        else:
            print("Error al realizar la solicitud. Código de estado:", response.status_code)

    return transformed_data

def get_data():
    url = "https://api.teleport.org/api/urban_areas/"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Error al realizar la solicitud. Código de estado:", response.status_code)

def load_data(**context):
    connection = create_connection()
    transformed_data = context['task_instance'].xcom_pull(task_ids='transformar_datos')

    insert_query = """
        INSERT INTO bapintor_coderhouse.ciudades (
            city,
            "Housing",
            "Cost of Living",
            "Startups",
            "Venture Capital",
            "Travel Connectivity",
            "Commute",
            "Business Freedom",
            "Safety",
            "Healthcare",
            "Education",
            "Environmental Quality",
            "Economy",
            "Taxation",
            "Internet Access",
            "Leisure & Culture",
            "Tolerance",
            "Outdoors"
            "process_date" 
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    cursor = connection.cursor()
    print("data transformada",transformed_data)
    for item in transformed_data:
        nombre = item["ciudad"]
        puntajes = item["puntajes"]

        p_values = [puntaje["score"] for puntaje in puntajes]

        cursor.execute(insert_query, (nombre, *p_values))

        try:
            connection.commit()
            print("listo comit")
        except Exception as e:
            connection.rollback()
            print("Error:", str(e))

    connection.close()



