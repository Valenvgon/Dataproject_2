import json
import random
import time
from faker import Faker
from google.cloud import pubsub_v1

# Configuración del proyecto y tópicos de Pub/Sub
PROJECT_ID = "data-project-2425"
TOPIC_VOLUNTARIOS = "projects/data-project-2425/topics/voluntarios_dana"
TOPIC_AFECTADOS = "projects/data-project-2425/topics/afectados_dana"

# Inicializar Faker para generar datos aleatorios
fake = Faker("es_ES")

TIPOS_AYUDA = ["Limpieza", "Transporte", "Primeros auxilios"]
HORARIOS = ["mañana", "tarde", "noche"]
MUNICIPIOS = ["Valencia", "Torrent", "Paterna"]

# Mensajes posibles para los afectados
MENSAJES_AFECTADOS = [
    "Necesito ayuda para limpiar mi casa después de la inundación.",
    "Se requiere transporte para evacuar personas mayores.",
    "Necesito asistencia médica urgente.",
    "Busco voluntarios para ayudar a distribuir comida en mi barrio."
]

publisher = pubsub_v1.PublisherClient()

def generar_voluntario():
    return {
        "id": fake.uuid4(),
        "nombre": fake.name(),
        "municipio": random.choice(MUNICIPIOS),
        "habilidades": random.sample(TIPOS_AYUDA, random.randint(1, 2)),
        "disponibilidad": {h: random.choice([True, False]) for h in HORARIOS}
    }

def generar_afectado():
    return {
        "id": fake.uuid4(),
        "municipio": random.choice(MUNICIPIOS),
        "tipo_ayuda": random.choice(TIPOS_AYUDA),
        "horario": random.choice(HORARIOS),
        "mensaje_solicitud": random.choice(MENSAJES_AFECTADOS)
    }

def publicar_mensaje(topic, mensaje):
    data = json.dumps(mensaje).encode("utf-8")
    publisher.publish(topic, data)
    print(f"Publicado en {topic}: {mensaje}")

if __name__ == "__main__":
    for _ in range(10):
        publicar_mensaje(TOPIC_VOLUNTARIOS, generar_voluntario())
        publicar_mensaje(TOPIC_AFECTADOS, generar_afectado())
        time.sleep(1)
