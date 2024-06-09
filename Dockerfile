# Uso la base oficial de Python
FROM python:3.9-slim

ENV AIRFLOW_HOME=/opt/airflow

# Instalo las dependencias
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libssl-dev \
    libffi-dev \
    libblas-dev \
    liblapack-dev \
    libpq-dev \
    git \
    ssh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Configuro el directorio de trabajo
WORKDIR $AIRFLOW_HOME

# Copio los archivos DAG al directorio de Airflow
COPY ./dags/ $AIRFLOW_HOME/dags/

# Inicializo la base de datos de Airflow y ejecuto el scheduler y el web server
CMD ["bash", "-c", "airflow db init && airflow webserver & airflow scheduler"]



