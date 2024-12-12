# Use the official Spark PySpark image
FROM apache/spark-py:latest

# Cambia al usuario root para resolver problemas de permisos
USER root

# Crear y ajustar permisos para el directorio de caché de pip
RUN mkdir -p /opt/spark/.cache/pip && \
    chmod -R 777 /opt/spark/.cache/pip && \
    mkdir -p /opt/spark/.local && \
    chmod -R 777 /opt/spark/.local && \
    chown -R 1000:1000 /opt/spark/.local

# Cambiar de nuevo al usuario spark
USER 1000

COPY requirements.txt .

ENV PYTHONUSERBASE=/opt/spark/.local
ENV PYTHONPATH=/opt/spark/.local/lib/python3.10/site-packages:$PYTHONPATH

# Instalar las dependencias con pip
RUN pip install --user --no-cache-dir -r requirements.txt

ENV NEW_DIR=/opt/spark/.local/lib/python3.10/site-packages
ENV PATH=$PATH:$NEW_DIR

ENV SPARK_DIR=/opt/spark/bin
ENV PATH\=$PATH:$SPARK_DIR

# Set the working directory inside the container
WORKDIR /app

# Expose Spark UI port
EXPOSE 4040