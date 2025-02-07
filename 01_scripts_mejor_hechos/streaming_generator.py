import random
import logging
import time
import faker as fk
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import argparse
from pubsub import PubSubMessages
from definitions import get_cities, get_messages_affected, generate_phone_number, get_messages_volunteers, disponibility_options, normalize_names

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
        "longitude": lon
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
    }

def run_streaming(project_id: str, affected_topic: str, volunteer_topic:str, num_affected_people: int):
    pubsub_class = PubSubMessages(project_id=project_id)
    affected_ids = {f"affected_{str(i).zfill(7)}" for i in range(1, num_affected_people + 1)}
    volunteer_ids = {f"volunteer_{str(i).zfill(7)}" for i in range(1, num_affected_people + 1)}
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
            time.sleep(10)
    
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

    parser.add_argument('--num_affected_people', 
        required=False, 
        default=200, 
        type=int, 
        help='Number of messages to send.'
    )

    
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting streaming data generator")
    run_streaming(args.project_id, args.affected_topic, args.volunteer_topic, args.num_affected_people)
    logging.info("Streaming data generator finished")




'''

to run the script:

python streaming_generator.py --project_id data-project-2425 --affected_topic affected --volunteer_topic volunteer


'''