# Entregable 3 Belén Pintor

En los log de los task del DAG se encuentran ciertos "prints" para confirmar que esté funcionando adecuadamente. A continuación se detallan los archivos presentes en la carpeta y las indicaciones para ejecutar el código. 

# Archivos
Los archivos de la carpeta son:
* `docker-compose.yml`: Archivo de configuración de Docker Compose. 
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres.
* `dags/`: Carpeta con los archivos de los DAGs y código, contiene los siguientes archivos.
    * `script_etl.py`: código python donde se definen las funciones que serán utilizadas en los task del DAG.
    * `dag_etl_paises.py`: DAG principal, ejecuta las funciones declaradas en el archivo dag_etl_paises.
    * `env.py`: lmacena las credenciales, sin embargo en última instancia se obtuvieron con el comando "Variable" definiendolas en la sesión de Airflow.
* `logs/`: Carpeta con los archivos de logs de Airflow.
* `plugins/`: Carpeta con los plugins de Airflow.
* `postgres_data/`: Carpeta con los datos de Postgres.


# Pasos para ejecutar el ejemplo
1. Posicionarse en la carpeta a la altura del archivo `docker-compose.yml`.

2. Ejecutar 
```bash
docker-compose up --build
```
3. En la sesión de Ariflow --> Admin --> Variables Definir las credenciales necesarias para acceder (Las actuales se encuentran en el archivo `.env`). 
```bash
DB_USER=...
DB_PASSWORD=...
DB_HOST=...
DB_PORT=...
DB_DATABASE=...
```

4. Ejecutar docker-compose up desde la carpeta y correr el DAG. 