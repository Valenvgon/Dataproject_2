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
import logging
import json

fake = fk.Faker('es_ES')

class PubSubMessages:

    """ Publish Messages in our PubSub Topic """

    def __init__(self, project_id: str):

        """
        Initialize the PubSubMessages class.

        Params:
            project_id(str): Google Cloud Project ID.

        Returns: 
            -
        """

        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id

        logging.info("PubSub Client initialized.")

    def publishMessages(self, payload: dict, topic_name: str):

        """
        Publishes the desired message to the specified topic.

        Params:
            payload(dict): Vehicle Telemetry Data Payload.
            topic_name(str): Google PubSub Topic Name.

        Returns: 
            -

        """

        json_str = json.dumps(payload).encode("utf-8")

        topic_path = self.publisher.topic_path(self.project_id, topic_name)
        self.publisher.publish(topic_path, json_str)

    def __exit__(self):
        
        self.publisher.transport.close()

        logging.info("PubSub Client closed.")

def get_cities():
    cities_list = [
        "valencia", 'albal'
    ]

    return cities_list


def get_messages_affected():
    messages = [

    ("suministros", "Un refugio temporal requiere productos de higiene personal y desinfectantes.", "productos_limpieza"),
    ("suministros", "Se solicita leche en polvo y pañales para bebes en un centro de acogida.", "comida_bebida"),
    ("botas_chanel", "He perdido mis botas chanel, dadme unas", "botas"),
    ("limpieza_calles/casas", "Las calles de un barrio estan cubiertas de lodo y se requieren palas para la limpieza.", "palas"),
    ("limpieza_calles/casas", "Un centro comunitario necesita escobas para retirar el barro acumulado en el suelo.", "escobas")
]
    
    return messages


def generate_phone_number():
    return f'+34-{random.randint(600000000, 699999999)}'


def get_messages_volunteers():
    messages= [
    ("suministros", "Somos un grupo de voluntarios y llevamos comida y agua potable para los afectados.", "comida_bebida"),
    ("capricho", "Quiero pasearme con mi bolso y hacerme una foto con alguien que lo haya perdido todo", "bolso_prada"),
    ("suministros", "Recolectamos ropa seca para distribuirla entre quienes la necesiten.", "ropa"),
    ("suministros", "Llevamos kits de higiene y productos de limpieza a los refugios.", "productos_limpieza"),
    ("suministros", "Tenemos alimentos no perecederos para repartir en zonas afectadas.", "comida_bebida"),
    ("suministros", "Estamos recolectando productos de higiene personal para entregar en los albergues.", "productos_limpieza")

    ]

    return messages 

def disponibility_options():
    options = ('manana', 'tarde')
    disponibility = random.choice(options)

    return disponibility

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
                    logging.warning(f"No se encontraron coordenadas para {city}. Se usará (0.0, 0.0).")
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

    fake = fk.Faker('es_ES')
    name = normalize_names(fake.name())
    phone = generate_phone_number()

    category, message, necessity = random.choice(messages)
    
    lat = city_data['latitude'] + random.uniform(-radius, radius)
    lon = city_data['longitude'] + random.uniform(-radius, radius)

    selected_city = city

    disponibility= disponibility_options()
    
    return {
        "affected_id": affected_id,
        "timestamp": timestamp_af.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": name,
        "phone": phone,
        "category": category,
        "message": message,
        "necessity": necessity,
        "city": selected_city,
        'disponibility': disponibility,
        "latitude": lat,
        "longitude": lon,
        'processed': 0
    }

def generate_volunteer_messages(volunteer_id, timestamp_vol, city_vol):
    messages= get_messages_volunteers()

    fake = fk.Faker('es_ES')
    name = normalize_names(fake.name())
    phone = generate_phone_number()
    category, message, necessity = random.choice(messages)

    selected_city= city_vol

    disponibility= disponibility_options()

    return {
        "volunteer_id": volunteer_id,
        "timestamp": timestamp_vol.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": name,
        "phone": phone,
        "category": category,
        "message": message,
        "necessity": necessity,
        "city": selected_city,
        'disponibility': disponibility,
        'processed': 0
    }

def run_streaming(project_id: str, affected_topic: str, volunteer_topic:str):
    pubsub_class = PubSubMessages(project_id=project_id)
    affected_ids = [f"affected_{str(i).zfill(5)}" for i in range(10000, 9999999)]
    volunteer_ids = [f"volunteer_{str(i).zfill(5)}" for i in range(10000, 9999999)]
    timestamps_af = {affected_id: datetime.now() for affected_id in affected_ids}
    timestamps_vol = {volunteer_id: datetime.now() for volunteer_id in volunteer_ids}
    city_coordinates, cities_list = get_city_coordinates()
    
    try:
        while True:
            affected_id = random.choice(list(affected_ids))
            volunteer_id = random.choice(list(volunteer_ids))
            selected_city = random.choice(cities_list)
            selected_city_vol = random.choice(cities_list)
            city_data = city_coordinates.get(selected_city, {"latitude": 0.0, "longitude": 0.0})
            event = generate_affected_messages(affected_id, timestamps_af[affected_id], selected_city, city_data)
            event_vol= generate_volunteer_messages(volunteer_id, timestamps_vol[volunteer_id], selected_city_vol)
            pubsub_class.publishMessages(payload=event, topic_name=affected_topic)
            pubsub_class.publishMessages(payload= event_vol, topic_name=volunteer_topic)
            logging.info(f"Published message for {affected_id} to {affected_topic}")
            logging.info(f"Published message for {volunteer_id} to {volunteer_topic}")
            timestamps_af[affected_id] += timedelta(seconds=random.randint(1, 60))
            timestamps_vol[volunteer_id] += timedelta(seconds=random.randint(1, 60))
            time.sleep(2)
    
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    logging.info("Probando obtención de coordenadas...")
    city_coordinates, cities_list = get_city_coordinates()
    
    for city, coords in city_coordinates.items():
        print(f"{city}: {coords}")


    parser = argparse.ArgumentParser(description="Generador de mensajes para personas afectadas en Valencia.")
    parser.add_argument('--project_id', 
        required=True,
        help='GCP Project ID.'
    )

    parser.add_argument('--affected_topic',
        required=True, 
        help='Topic name for affected messages.'
    )

    parser.add_argument('--volunteer_topic',
        required= True, 
        help='Topic name for volunteer messages')
    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting streaming data generator")
    run_streaming(args.project_id, args.affected_topic, args.volunteer_topic)
    logging.info("Streaming data generator finished")




'''

to run the script:

python streaming_generator_tocao.py --project_id data-project-2425 --affected_topic affected --volunteer_topic volunteer

'''