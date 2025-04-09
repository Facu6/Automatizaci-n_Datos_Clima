# Usa la imagen base de Airflow
FROM apache/airflow:2.3.3

# Establecer el directorio de trabajo para Airflow
WORKDIR /opt/airflow

# Elevar permisos a ROOT para actualizar paquetes
USER root

# Corregir permisos y actualizar paquetes
RUN chmod 777 /var/lib/apt/lists && \
    apt-get update --allow-releaseinfo-change && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Volver al usuario AIRFLOW (Obligatorio para que Airflow funcione)
USER airflow

# Instalar librerías necesarias para correr el proyecto
RUN pip install --no-cache-dir \
    pandas \
    requests \
    pymysql \
    pyspark \
    python-dotenv

# Comando por defecto al iniciar el contenedor 
CMD ["airflow", "webserver"]