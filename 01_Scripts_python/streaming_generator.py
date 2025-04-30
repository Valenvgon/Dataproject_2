import random
import logging
import time
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
import argparse
from pubsub import PubSubMessages

parser = argparse.ArgumentParser(description=('Generador de mensajes de demandantes de ayuda en las ciudades afectadas por la Dana en Valencia.'))

parser.add_argument('--project_id', required=True, help='GCP Project ID.')
parser.add_argument('--affected_topic', required=True, help='Topic name for affected people.')
parser.add_argument('--city_name', required=True, help='City name of affected people.')
parser.add_argument('--num_affected_people', required=False, default=5, type=int, help='Number of messages to send.')

args, opts = parser.parse_known_args()

def get_city_coordinates(city_name: str):
    geolocator = Nominatim(user_agent="geoapi")
    try:
        location = geolocator.geocode(city_name)
        if location:
            return {"latitude": location.latitude, "longitude": location.longitude}
        else:
            raise ValueError(f"No coordinates found for city: {city_name}")
    except Exception as e:
        logging.error(f"Error retrieving coordinates: {e}")
        return {"latitude": 0.0, "longitude": 0.0}

def generate_affected_message(affected_id, timestamp, city_coordinates, radius=0.005):
    messages = [
        ("Juan Pérez", "123-456-7890", "Limpieza", "Necesitamos voluntarios para limpiar calles inundadas."),
        ("María Gómez", "987-654-3210", "Suministros básicos", "Se requiere ropa y comida para personas evacuadas."),
        ("Carlos López", "456-123-7890", "Asistencia médica", "Buscamos ayuda para atender a enfermos y heridos."),
        ("Ana Ramírez", "321-654-9870", "Maquinaria pesada", "Urgente: Se necesita maquinaria pesada para retirar escombros."),
    ]
    
    name, phone, category, message = random.choice(messages)
    lat = city_coordinates['latitude'] + random.uniform(-radius, radius)
    lon = city_coordinates['longitude'] + random.uniform(-radius, radius)
    
    return {
        "affected_id": affected_id,
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": name,
        "phone": phone,
        "category": category,
        "message": message,
        'latitude': lat,
        'longitude': lon
    }

def run_streaming(project_id, affected_topic, city_name, num_affected_people):
    pubsub_class = PubSubMessages(project_id=project_id)
    affected_ids = [f"affected_{str(i).zfill(3)}" for i in range(1, num_affected_people + 1)]
    timestamps = {affected_id: datetime.now() for affected_id in affected_ids}
    city_coordinates = get_city_coordinates(city_name)
    
    try:
        while True:
            affected_id = random.choice(affected_ids)
            event = generate_affected_message(affected_id, timestamps[affected_id], city_coordinates)
            pubsub_class.publishMessages(payload=event, topic_name=affected_topic)
            logging.info(f"Published message for {affected_id} to {affected_topic}")
            timestamps[affected_id] += timedelta(seconds=random.randint(1, 60))
            time.sleep(50)
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    logging.info(f"Running streaming data generator for affected people in {args.city_name}")
    run_streaming(args.project_id, args.affected_topic, args.city_name, args.num_affected_people)
    logging.info("Streaming data generator finished")