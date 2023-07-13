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

def create_table():
    connection = create_connection()
 
    schema = "bapintor_coderhouse"
    table_name = "ciudades"
    #Creación de tabla (ya la cree en mi esquema)
    #city es la primary key porque en esta data no existen dos registros para la misma ciudad. 
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
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
        "Outdoors" DECIMAL(10, 2)
    )
    -- Aplicar la clave de ordenación a la nueva tabla
    SORTKEY (city);
    """
    cursor=connection.cursor()
    # Ejecuta la consulta CREATE TABLE
    cursor.execute(create_table_query)
    # Confirma los cambios en la base de datos
    connection.commit()

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
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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



