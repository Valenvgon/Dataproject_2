# Usamos una imagen base de Python
FROM python:3.9-slim

# Configuramos el directorio de trabajo
WORKDIR /app

# Copiamos los archivos necesarios
COPY requirements.txt requirements.txt
COPY app.py app.py

# Instalamos las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Exponemos el puerto de Streamlit
EXPOSE 8501

# Comando para ejecutar Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]