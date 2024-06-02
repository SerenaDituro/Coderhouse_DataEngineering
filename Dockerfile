# FROM apache/airflow:2.2.4-python3.8
# Parto de la imagen oficial de Python
FROM python:3.9-slim

ENV AIRFLOW_HOME=/opt/airflow

# Instalación de dependencias
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
# WORKDIR $AIRFLOW_HOME

# Defino las variables de entorno para el usuario y la contraseña
# ENV AIRFLOW_UID=50000
# ENV AIRFLOW_GID=0

# ENV AIRFLOW__WEBSERVER__AUTHENTICATE=True
# ENV AIRFLOW__WEBSERVER__AUTH_BACKEND=airflow.contrib.auth.backends.password_auth
# ENV AIRFLOW__WEBSERVER__USERNAME=airflow
# ENV AIRFLOW__WEBSERVER__PASSWORD=airflow

# ENV _AIRFLOW_WWW_USER_USERNAME=airflow
# ENV _AIRFLOW_WWW_USER_PASSWORD=airflow

# Copio los archivos DAG al directorio de Airflow
COPY ./dags/ $AIRFLOW_HOME/dags/

# Inicializo la base de datos de Airflow y ejecuto el scheduler y el web server
CMD ["bash", "-c", "airflow webserver & airflow scheduler"]


# # Imagen base de Python
# FROM python:3.9-slim

# # Instalar dependencias del sistema
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     libssl-dev \
#     libffi-dev \
#     libpq-dev \
#     && apt-get clean

# # Directorio de trabajo
# WORKDIR /app

# # Copiar e instalar las dependencias de Python
# COPY requirements.txt requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt

# # Copiar los DAGs
# COPY dags/ dags/

# # Crear directorios necesarios y configurar permisos
# RUN mkdir -p /airflow/logs /airflow/dags /airflow/plugins && \
#     chmod -R 777 /airflow/logs /airflow/dags /airflow/plugins

# # Configurar el entrypoint
# ENTRYPOINT ["airflow"]



