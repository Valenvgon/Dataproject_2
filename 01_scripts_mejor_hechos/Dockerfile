# Usar una imagen base con Python 3.10.16
FROM python:3.10.16-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar los archivos necesarios al contenedor
COPY streaming_generator.py streaming_generator.py
COPY requirements.txt requirements.txt

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Comando por defecto para ejecutar el script con argumentos de ejemplo
CMD ["python", "streaming_generator.py", "--project_id", "data-project-2425", "--affected_topic", "afectados_dana", "--volunteer_topic", "voluntarios_dana"]
