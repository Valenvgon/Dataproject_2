import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import logging

# Configuración del proyecto y Pub/Sub
PROJECT_ID = "data-project-2425"
TOPIC_VOLUNTARIOS = f"projects/{PROJECT_ID}/topics/voluntarios_dana"
TOPIC_AFECTADOS = f"projects/{PROJECT_ID}/topics/afectados_dana"
TOPIC_MATCHES = f"projects/{PROJECT_ID}/topics/matches"
TOPIC_NO_MATCH = f"projects/{PROJECT_ID}/topics/no_match"

# Ruta de almacenamiento en Google Cloud Storage
TEMP_LOCATION = f"gs://{PROJECT_ID}-temp/"
STAGING_LOCATION = f"gs://{PROJECT_ID}-staging/"

# Definir TupleTags para las salidas múltiples
MATCHES_TAG = "matches"
NO_MATCH_TAG = "sin_match"

# Configurar logging
logging.basicConfig(level=logging.INFO)

class ParseJson(beam.DoFn):
    def process(self, element):
        try:
            data = json.loads(element.decode("utf-8"))
            logging.info(f"Mensaje recibido: {data}")
            yield data
        except Exception as e:
            logging.error(f"Error al parsear JSON: {e}")

class MatchVoluntariosAfectados(beam.DoFn):
    def process(self, afectado, voluntarios):
        ciudad = afectado["city"]
        necesidad = afectado["necessity"]
        disponibilidad = afectado["disponibility"]

        voluntarios_filtrados = [
            v for v in voluntarios
            if v["city"] == ciudad and necesidad == v["necessity"] and disponibilidad == v["disponibility"]
        ]

        if voluntarios_filtrados:
            match = {
                "afectado": afectado,
                "voluntario": voluntarios_filtrados[0]
            }
            logging.info(f"MATCH encontrado: {match}")
            yield beam.pvalue.TaggedOutput(MATCHES_TAG, match)
        else:
            logging.info(f"NO MATCH para afectado: {afectado}")
            yield beam.pvalue.TaggedOutput(NO_MATCH_TAG, afectado)

def run():
    pipeline_options = PipelineOptions(
        streaming=True, project=PROJECT_ID, region="europe-southwest1",
        runner="DataflowRunner", temp_location=TEMP_LOCATION, staging_location=STAGING_LOCATION
    )

    with beam.Pipeline(options=pipeline_options) as p:
        voluntarios = (
            p 
            | "Leer Voluntarios" >> beam.io.ReadFromPubSub(topic=TOPIC_VOLUNTARIOS)
            | "Parsear Voluntarios JSON" >> beam.ParDo(ParseJson())
        )

        afectados = (
            p
            | "Leer Afectados" >> beam.io.ReadFromPubSub(topic=TOPIC_AFECTADOS)
            | "Parsear Afectados JSON" >> beam.ParDo(ParseJson())
        )

        resultados = (
            afectados 
            | "Buscar Matches" >> beam.ParDo(MatchVoluntariosAfectados(), beam.pvalue.AsList(voluntarios))
            .with_outputs(MATCHES_TAG, NO_MATCH_TAG)
        )

        matches = resultados[MATCHES_TAG]
        sin_match = resultados[NO_MATCH_TAG]

        matches | "Enviar Matches a Pub/Sub" >> beam.io.WriteToPubSub(TOPIC_MATCHES)
        sin_match | "Enviar No-Match a Pub/Sub" >> beam.io.WriteToPubSub(TOPIC_NO_MATCH)
        
if __name__ == "__main__":
    run()





