import random
import logging
import time
import faker as fk
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import argparse
from pubsub import PubSubMessages

def get_city_coordinates():
    """
    Obtiene las coordenadas (latitud y longitud) de una lista predefinida de ciudades.
    """
    cities_list = [
        "valencia", "torrent", "paiporta", "aldaia", "alfafar", "benetusser", "catarroja", "chiva", "massanassa", "albal",
        "silla", "alcasser", "picassent", "albalat_de_la_ribera", "alborache", "alcudia", "alginet", "bunyol",
        "catadau", "cheste", "godelleta", "guadassuar", "loriguilla", "almussafes", "alzira", "benifaio", "beniparrell",
        "betera", "bugarra", "calles", "camporrobles", "carlet", "corbera", "quart_de_poblet", "cullera", "chera",
        "dos_aguas", "xirivella", "albal", "benetusser", "catarroja", "chiva", "massanassa", "paiporta"
    ]
    
    geolocator = Nominatim(user_agent="geoapi", timeout=10)
    city_coordinates = {}

    for city in cities_list:
        attempts = 3  # Número de reintentos
        for attempt in range(attempts):
            try:
                location = geolocator.geocode(city, timeout=10)  # Se asegura de no bloquear
                if location:
                    city_coordinates[city] = {"latitude": location.latitude, "longitude": location.longitude}
                else:
                    logging.warning(f"No se encontraron coordenadas para {city}. Se usará (0.0, 0.0).")
                    city_coordinates[city] = {"latitude": 0.0, "longitude": 0.0}
                break  # Salir del intento si se obtiene una respuesta válida
            except GeocoderTimedOut:
                logging.warning(f"Tiempo de espera agotado para {city}. Reintentando... ({attempt+1}/{attempts})")
                time.sleep(2)  # Esperar antes de reintentar
            except Exception as e:
                logging.error(f"Error obteniendo coordenadas de {city}: {e}")
                city_coordinates[city] = {"latitude": 0.0, "longitude": 0.0}
                break  # Salir del intento si ocurre otro error
    
    return city_coordinates, cities_list


def generate_phone_number():
    return f'+34-{random.randint(600000000, 699999999)}'

def generate_affected_message(affected_id, timestamp, city_data, radius=0.005):

    messages = [
    ("Maquinaria pesada", "Se necesita una excavadora para remover escombros en una calle bloqueada.", "Excavadora"),
    ("Maquinaria pesada", "Buscamos un camion para trasladar los restos de una casa derrumbada.", "Camion"),
    ("Maquinaria pesada", "Requerimos maquinaria para despejar un camino cubierto de lodo y arboles caidos.", "Bulldozer"),
    ("Maquinaria pesada", "Urgente: Se necesita una grua para levantar un vehiculo atrapado en el agua.", "Grua"),
    ("Maquinaria pesada", "Solicitamos ayuda con una retroexcavadora para retirar escombros en el barrio afectado.", "Retroexcavadora"),

    ("Ayuda medica", "Una persona con problemas respiratorios necesita oxigeno y asistencia medica.", "Oxigeno"),
    ("Ayuda medica", "Se requiere personal de salud para atender a personas con hipotermia.", "Personal medico"),
    ("Ayuda medica", "Urgente: Un nino con fiebre alta necesita ser trasladado a un hospital.", "Ambulancia"),
    ("Ayuda medica", "Un anciano con diabetes ha perdido su medicacion y necesita insulina.", "Insulina"),
    ("Ayuda medica", "Se necesita un medico para revisar a varias personas con heridas leves.", "Medico"),

    ("Ayuda a ancianos", "Un matrimonio de ancianos se ha quedado sin electricidad y necesita asistencia.", "Generador electrico"),
    ("Ayuda a ancianos", "Un anciano con movilidad reducida necesita ser evacuado de su vivienda.", "Silla de ruedas"),
    ("Ayuda a ancianos", "Solicitamos voluntarios para llevar comida y agua a ancianos que no pueden salir.", "Voluntarios"),
    ("Ayuda a ancianos", "Un grupo de ancianos en una residencia necesita mantas y alimentos.", "Alimentos"),
    ("Ayuda a ancianos", "Una persona mayor con discapacidad necesita ayuda para salir de su casa inundada.", "Vehiculo de rescate"),

    ("Falta de suministros", "Necesitamos alimentos no perecederos para familias desplazadas.", "Alimentos"),
    ("Falta de suministros", "Varias familias han perdido su ropa y necesitan ropa de abrigo.", "Ropa"),
    ("Falta de suministros", "Urgente: Se necesita agua potable en un refugio con 50 personas.", "Agua potable"),
    ("Falta de suministros", "Ninos pequenos necesitan panales y leche en polvo.", "Panales y leche en polvo"),
    ("Falta de suministros", "Un albergue improvisado requiere mantas y productos de higiene.", "Mantas y productos de higiene"),

    ("Limpieza", "Se necesitan voluntarios para limpiar el barro de una escuela inundada.", "Escobas y palas"),
    ("Limpieza", "Buscamos ayuda para retirar escombros y lodo de una casa danada.", "Picos y palas"),
    ("Limpieza", "Varias calles estan llenas de restos de arboles y necesitan limpieza urgente.", "Motosierras"),
    ("Limpieza", "Se requiere ayuda para desinfectar un refugio temporal despues de la tormenta.", "Desinfectantes"),
    ("Limpieza", "Solicitamos escobas, palas y desinfectantes para limpiar un centro comunitario.", "Escobas y desinfectantes"),

    ("Planta baja inundada", "Una familia necesita ayuda para achicar el agua de su casa anegada.", "Bomba de agua"),
    ("Planta baja inundada", "El sotano de un hospital esta inundado y requiere bombas de agua.", "Bombas de agua"),
    ("Planta baja inundada", "Un local comercial necesita ser drenado para evitar mas danos materiales.", "Motobomba"),
    ("Planta baja inundada", "Se necesita apoyo con cubetas y bombas para sacar agua de varias viviendas.", "Cubetas y bombas"),
    ("Planta baja inundada", "Una casa con personas mayores esta inundada y requiere asistencia inmediata.", "Bomba extractora"),

    ("Falta de suministros", "Un refugio con 30 personas necesita alimentos y agua.", "Alimentos y agua"),
    ("Falta de suministros", "Se solicita ropa seca para ninos y adultos afectados.", "Ropa seca"),
    ("Ayuda medica", "Un paciente con hipertension ha perdido su medicacion y necesita atencion.", "Medicamentos"),
    ("Ayuda a ancianos", "Un anciano sin familiares necesita ayuda para desplazarse a un refugio.", "Transporte"),
    ("Limpieza", "Varias casas necesitan ser limpiadas despues de la tormenta para prevenir enfermedades.", "Agua y desinfectante"),
    ("Planta baja inundada", "Un negocio ha perdido todos sus productos por la inundacion y requiere apoyo.", "Secadoras y deshumidificadores"),
    ("Maquinaria pesada", "Una excavadora es necesaria para retirar lodo de una carretera principal.", "Excavadora"),
    ("Ayuda a ancianos", "Un grupo de ancianos con movilidad reducida necesita transporte a un refugio.", "Autobus adaptado")
]


    fake = fk.Faker('es_ES')
    name = fake.name()
    phone = generate_phone_number()
    category, message, necessity = random.choice(messages)
    
    lat = city_data['latitude'] + random.uniform(-radius, radius)
    lon = city_data['longitude'] + random.uniform(-radius, radius)
    
    return {
        "affected_id": affected_id,
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "name": name,
        "phone": phone,
        "category": category,
        "message": message,
        "necessity": necessity,
        "latitude": lat,
        "longitude": lon
    }

def run_streaming(project_id: str, affected_topic: str, num_affected_people: int):
    pubsub_class = PubSubMessages(project_id=project_id)
    affected_ids = {f"affected_{str(i).zfill(7)}" for i in range(1, num_affected_people + 1)}
    timestamps = {affected_id: datetime.now() for affected_id in affected_ids}
    city_coordinates, cities_list = get_city_coordinates()
    
    try:
        while True:
            affected_id = random.choice(list(affected_ids))
            selected_city = random.choice(cities_list)
            city_data = city_coordinates.get(selected_city, {"latitude": 0.0, "longitude": 0.0})
            event = generate_affected_message(affected_id, timestamps[affected_id], city_data)
            pubsub_class.publishMessages(payload=event, topic_name=affected_topic)
            logging.info(f"Published message for {affected_id} to {affected_topic}")
            timestamps[affected_id] += timedelta(seconds=random.randint(1, 60))
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
    parser.add_argument('--project_id', required=True, help='GCP Project ID.')
    parser.add_argument('--affected_topic', required=True, help='Topic name for affected people.')
    parser.add_argument('--num_affected_people', required=False, default=200, type=int, help='Number of messages to send.')
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting streaming data generator")
    run_streaming(args.project_id, args.affected_topic, args.num_affected_people)
    logging.info("Streaming data generator finished")




'''

to run the script:

python streaming_generator.py --project_id data-project-2425 --affected_topic affected_topic


'''