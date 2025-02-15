import random
import logging
import time
import faker as fk
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import argparse
import unidecode
from google.cloud import pubsub_v1
import json

fake = fk.Faker('es_ES')

class PubSubMessages:
    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id: str):
        """
        Initialize the PubSubMessages class.

        Params:
            project_id(str): Google Cloud Project ID.
        """
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        logging.info("PubSub Client initialized.")

    def publishMessages(self, payload: dict, topic_name: str):
        """
        Publishes the desired message to the specified topic.

        Params:
            payload(dict): Data Payload.
            topic_name(str): Google PubSub Topic Name.
        """
        json_str = json.dumps(payload).encode("utf-8")
        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        self.publisher.publish(topic_path, json_str)
        logging.info(f"Published message to {topic_name}: {payload}")

    def __exit__(self):
        self.publisher.transport.close()
        logging.info("PubSub Client closed.")

def get_cities():
    return ["valencia", "albal", "paiporta", "alfafar", "catarroja"]

def get_messages_affected():
    messages = [
        ("suministros", "Un refugio temporal requiere productos de higiene personal y desinfectantes.", "productos_limpieza"),
        ("suministros", "Se solicita leche en polvo y panales para bebes en un centro de acogida.", "comida_bebida"),
        ("botas", "He perdido mis botas, dadme unas", "botas"),
        ("limpieza", "Las calles de un barrio estan cubiertas de lodo y se requieren palas para la limpieza.", "palas"),
        ("limpieza", "Un centro comunitario necesita escobas para retirar el barro acumulado en el suelo.", "escobas"),
        ("medicinas", "Un centro de salud necesita medicamentos esenciales para pacientes afectados.", "medicinas"),
        ("transporte", "Se necesita transporte para evacuar personas de areas afectadas.", "transporte"),
        ("refugio", "Familias desplazadas necesitan albergue temporal.", "refugio")
    ]
    return [(unidecode.unidecode(cat), unidecode.unidecode(msg), unidecode.unidecode(nec)) for cat, msg, nec in messages]

def get_messages_volunteers():
    messages = [
        ("suministros", "Somos un grupo de voluntarios y llevamos comida y agua potable para los afectados.", "comida_bebida"),
        ("capricho", "Quiero pasearme con mi bolso y hacerme una foto con alguien que lo haya perdido todo", "bolso"),
        ("suministros", "Recolectamos ropa seca para distribuirla entre quienes la necesiten.", "ropa"),
        ("suministros", "Llevamos kits de higiene y productos de limpieza a los refugios.", "productos_limpieza"),
        ("suministros", "Tenemos alimentos no perecederos para repartir en zonas afectadas.", "comida_bebida"),
        ("suministros", "Estamos recolectando productos de higiene personal para entregar en los albergues.", "productos_limpieza"),
        ("medicinas", "Disponemos de medicamentos esenciales para entregar en centros de salud.", "medicinas"),
        ("transporte", "Tenemos vehiculos disponibles para ayudar en la evacuacion de personas.", "transporte"),
        ("refugio", "Ofrecemos alojamiento temporal para familias desplazadas.", "refugio")
    ]
    return [(unidecode.unidecode(cat), unidecode.unidecode(msg), unidecode.unidecode(nec)) for cat, msg, nec in messages]

def generate_phone_number():
    return f'+34-{random.randint(600000000, 699999999)}'

def disponibility_options():
    return random.choice(["manana", "tarde"])

def normalize_names(name):
    return unidecode.unidecode(name)

def get_city_coordinates():
    """
    Obtiene las coordenadas (latitud y longitud) de una lista predefinida de ciudades.
    """
    cities_list = get_cities()
    geolocator = Nominatim(user_agent="geoapi", timeout=10)
    city_coordinates = {}

    for city in cities_list:
        attempts = 3  
        for attempt in range(attempts):
            try:
                location = geolocator.geocode(city, timeout=10)
                if location:
                    city_coordinates[city] = {"latitude": location.latitude, "longitude": location.longitude}
                else:
                    logging.warning(f"No se encontraron coordenadas para {city}. Se usara (0.0, 0.0).")
                    city_coordinates[city] = {"latitude": 0.0, "longitude": 0.0}
                break  
            except GeocoderTimedOut:
                logging.warning(f"Tiempo de espera agotado para {city}. Reintentando... ({attempt+1}/{attempts})")
                time.sleep(2)
            except Exception as e:
                logging.error(f"Error obteniendo coordenadas de {city}: {e}")
                city_coordinates[city] = {"latitude": 0.0, "longitude": 0.0}
                break  

    return city_coordinates, cities_list 

def generate_affected_messages(affected_id, timestamp_af, city, city_data, radius=0.005):
    messages = get_messages_affected()
    name = normalize_names(fake.name())
    phone = generate_phone_number()
    category, message, necessity = random.choice(messages)

    lat = city_data["latitude"] + random.uniform(-radius, radius)
    lon = city_data["longitude"] + random.uniform(-radius, radius)
    disponibility = disponibility_options()
    
    return {
        "affected_id": affected_id,
        "timestamp": timestamp_af.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": name,
        "phone": phone,
        "category": category,
        "message": message,
        "necessity": necessity,
        "city": city,
        "disponibility": disponibility,
        "latitude": lat,
        "longitude": lon,
        "processed": 0
    }

def generate_volunteer_messages(volunteer_id, timestamp_vol, city_vol):
    messages = get_messages_volunteers()
    name = normalize_names(fake.name())
    phone = generate_phone_number()
    category, message, necessity = random.choice(messages)

    disponibility = disponibility_options()

    return {
        "volunteer_id": volunteer_id,
        "timestamp": timestamp_vol.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": name,
        "phone": phone,
        "category": category,
        "message": message,
        "necessity": necessity,
        "city": city_vol,
        "disponibility": disponibility,
        "processed": 0
    }

def run_streaming(project_id: str, affected_topic: str, volunteer_topic:str):
    pubsub_class = PubSubMessages(project_id=project_id)
    timestamps_af = {}
    timestamps_vol = {}
    city_coordinates, cities_list = get_city_coordinates()

    try:
        while True:
            affected_id = f"affected_{random.randint(10000, 9999999)}"
            volunteer_id = f"volunteer_{random.randint(10000, 9999999)}"
            city = random.choice(cities_list)
            city_data = city_coordinates[city]

            event = generate_affected_messages(affected_id, datetime.now(), city, city_data)
            event_vol = generate_volunteer_messages(volunteer_id, datetime.now(), city)
            pubsub_class.publishMessages(payload=event, topic_name=affected_topic)
            pubsub_class.publishMessages(payload=event_vol, topic_name=volunteer_topic)
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_streaming("data-project-2425", "affected", "volunteer")




'''

to run the script:

python streaming_generator_tocao.py --project_id data-project-2425 --affected_topic affected --volunteer_topic volunteer

'''