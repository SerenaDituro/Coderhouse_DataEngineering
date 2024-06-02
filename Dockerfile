# Usa la imagen oficial de Airflow como base
FROM apache/airflow:2.2.4-python3.8

# Configura el directorio de trabajo
WORKDIR /opt/airflow

# Copia el archivo requirements.txt al contenedor
COPY requirements.txt .

# Instala las dependencias adicionales especificadas en requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copiar los archivos de los DAGs al contenedor
COPY dags/ /opt/airflow/dags/

# Establece el entrypoint para los comandos de Airflow
ENTRYPOINT ["tini", "--", "airflow"]

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



