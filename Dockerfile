FROM apache/airflow:2.3.3


USER root

RUN python -m pip install --upgrade pip
RUN python -m pip install apache-airflow-providers-apache-spark
RUN python -m pip install pg8000

USER airflow

RUN python -m pip install --upgrade pip
RUN python -m pip install pg8000
RUN pip install pg8000
RUN python -m pip install python-dotenv


COPY ./requirements.txt /
RUN python -m pip install -r /requirements.txt
RUN pip install -r /requirements.txt

