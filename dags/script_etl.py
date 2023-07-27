import os
import requests
import psycopg2
import logging
import pandas as pd
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

from os import environ as env

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
            "process_date"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
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

    connection = psycopg2.connect(
        user=Variable.get("DB_USER"),
        password=Variable.get("DB_PASSWORD"),
        host=Variable.get("DB_HOST"),
        port=Variable.get("DB_PORT"),
        database=Variable.get("DB_DATABASE")
    )
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