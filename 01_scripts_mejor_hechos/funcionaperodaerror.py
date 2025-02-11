import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import logging
import argparse

# Configuración del proyecto y Pub/Sub
PROJECT_ID = "data-project-2425"
TOPIC_VOLUNTARIOS = f"projects/{PROJECT_ID}/topics/voluntarios_prueba"
TOPIC_AFECTADOS = f"projects/{PROJECT_ID}/topics/afectados_prueba"
TOPIC_MATCHES = f"projects/{PROJECT_ID}/topics/matches"
TOPIC_NO_MATCH = f"projects/{PROJECT_ID}/topics/no_match"

# Ruta de almacenamiento en Google Cloud Storage
TEMP_LOCATION = f"gs://dataflow_bucket_dataproject_2425/temp"
STAGING_LOCATION = f"gs://dataflow_bucket_dataproject_2425/staging"

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

        import apache_beam as beam

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

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--voluntarios_topic',
                required=True,
                help='PubSub subscription used for reading battery telemetry data.')
    
    parser.add_argument(
                '--afectados_topic',
                required=True,
                help='PubSub subscription used for reading driving telemetry data.')
    
    parser.add_argument(
                '--output_topic',
                required=True,
                help='PubSub Topic for sending push notifications.')

    args, pipeline_opts = parser.parse_known_args()

    
    pipeline_options = PipelineOptions(
        streaming=True, project=PROJECT_ID, region="europe-southwest1",
        runner="DataflowRunner", temp_location=TEMP_LOCATION, staging_location=STAGING_LOCATION
    )

    with beam.Pipeline(options=pipeline_options) as p:
        voluntarios = (
            p 
            | "Leer Voluntarios" >> beam.io.ReadFromPubSub(topic=TOPIC_VOLUNTARIOS)
            | "Parsear Voluntarios JSON" >> beam.ParDo(ParseJson())
            | "Fixed Window Voluntarios" >> beam.WindowInto(beam.window.FixedWindows(90))
        )

        afectados = (
            p
            | "Leer Afectados" >> beam.io.ReadFromPubSub(topic=TOPIC_AFECTADOS)
            | "Parsear Afectados JSON" >> beam.ParDo(ParseJson())
            | "Fixed Window Afectados" >> beam.WindowInto(beam.window.FixedWindows(90))
        )

        # Aplicar matching
        resultados = (
            afectados 
            | "Buscar Matches" >> beam.ParDo(MatchVoluntariosAfectados(), beam.pvalue.AsList(voluntarios))
            .with_outputs(MATCHES_TAG, NO_MATCH_TAG)
        )

        matches = resultados[MATCHES_TAG] 
        sin_match = resultados[NO_MATCH_TAG]

        # Enviar Matches a Pub/Sub
        matches | "Formato Matches JSON" >> beam.Map(lambda x: json.dumps(x).encode("utf-8")) \
                | "Enviar Matches a Pub/Sub" >> beam.io.WriteToPubSub(TOPIC_MATCHES)

        # Enviar No Matches a Pub/Sub
        sin_match | "Formato No-Matches JSON" >> beam.Map(lambda x: json.dumps(x).encode("utf-8")) \
                  | "Enviar No-Match a Pub/Sub" >> beam.io.WriteToPubSub(TOPIC_NO_MATCH)

if __name__ == "__main__":
    run()
