# Proyecto Final Belén Pintor

En los log de los task del DAG se encuentran ciertos "prints" para confirmar que esté funcionando adecuadamente. A continuación se detallan los archivos presentes en la carpeta y las indicaciones para ejecutar el código. 

# Archivos
Los archivos de la carpeta son:
* `docker-compose.yml`: Archivo de configuración de Docker Compose. 
* `.env`: Archivo de variables de entorno. En este caso se optó por definirlas en el webserver de airflow pero puede utilizarse este también.
* `dags/`: Carpeta con los archivos de los DAGs y código, contiene los siguientes archivos.
    * `script_etl.py`: código python donde se definen las funciones que serán utilizadas en los task del DAG.
    * `dag_etl_paises.py`: DAG principal, ejecuta las funciones declaradas en el archivo dag_etl_paises.
* `logs/`: Carpeta con los archivos de logs de Airflow. Para no llenar de archivos se añadió en el gitignore
* `plugins/`: Carpeta con los plugins de Airflow. También se encuentra en el gitignore
* `Dockerfile`: Archivo con las librerías necesarias* *`requeriments.txt`: Detalles liberrías necesarias . 



# Pasos para ejecutar el ejemplo    
1. Posicionarse en la carpeta a la altura del archivo `docker-compose.yml`.

2. Ejecutar 
    ```bash
    docker-compose up --build
    ```
3. En la sesión de Ariflow deben configurarse variables y una conexión de la siguiente manera. 

    3.1 `Variables`
Definirlas credenciales necesarias SMTP, en este caso estan definidas así: 
    ```bash
    SMTP_EMAIL_FROM
    SMTP_EMAIL_TO
    SMTP_HOST
    SMTP_PASSWORD
    SMTP_HOST
    ```    
  
    3.2 `Conexiones`: En Admin --> coneiones debe crearse una conexión a la DB de Amazon redshift. La cual en este caso se definió bajo el siguiente id:
    ```bash
    Conn_id= redshift_belen
    ```    



4. Ejecutar docker-compose up desde la carpeta y correr el DAG. 

* Además en el comienzo de `script_etl.py` se encuentra un diccionario para modificar las ciudades y valores límites de interes que gatillarán luego alertas al correo. 

# Funciones y DAG 
La tarea cuenta con 5 DAGS. 
* Los primeros 3 son procesos ETL de creación de tabla extracción de datos, transformación y carga. 
* Luego se verifican los thresholds definidos en un diccionario (los cuales son modificables en este e incluso se pueden agregar más) y se envía un correo de notificación 
* Y por último también se dejo habilitado un task que envía un correo de succes cuando termina el procedimiento.


Imagenes de correos:
![image](https://github.com/belenpintor/EntregaFinal_BelenPintor_DATENG_51935/assets/69732485/ef4af468-9815-4b76-b9ca-3ffb9840bf33)
![image](https://github.com/belenpintor/EntregaFinal_BelenPintor_DATENG_51935/assets/69732485/48fe36a3-63f2-45e2-a2bb-feb6ee49279e)




