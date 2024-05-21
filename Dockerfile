# Imagen de Python
FROM python:3.9-slim

# Directorio de trabajo
WORKDIR /app

# Copia e instala las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia los archivos de la aplicación
COPY . .

# Configura el entrypoint
ENTRYPOINT ["airflow"]
