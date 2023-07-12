import psycopg2
import os

try:
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_database = os.getenv("DB_DATABASE")

    # Crear conexión a la base de datos
    connection = psycopg2.connect(
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_database
    )
    print("Conexión exitosa a PostgreSQL")
    connection.close()
except (Exception, psycopg2.Error) as error:
    print("Error al conectar a PostgreSQL:", error)
