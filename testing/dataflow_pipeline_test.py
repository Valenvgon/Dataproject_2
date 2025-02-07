import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

# Configuración del proyecto y Pub/Sub
PROJECT_ID = "data-project-2425"
TOPIC_VOLUNTARIOS = "projects/data-project-2425/topics/voluntarios_dana"
TOPIC_AFECTADOS = "projects/data-project-2425/topics/afectados_dana"
TOPIC_MATCHES = "projects/data-project-2425/topics/matches"
TOPIC_NO_MATCH = "projects/data-project-2425/topics/no_match"

# Ruta de almacenamiento en Google Cloud Storage
TEMP_LOCATION = "gs://data-project-2425-temp/"
STAGING_LOCATION = "gs://data-project-2425-staging/"

# Definir TupleTags para las salidas múltiples
MATCHES_TAG = "matches"
NO_MATCH_TAG = "sin_match"

class ParseJson(beam.DoFn):
    def process(self, element):
        yield json.loads(element.decode("utf-8"))

class MatchVoluntariosAfectados(beam.DoFn):
    def process(self, afectado, voluntarios):
        ciudad = afectado["municipio"]
        necesidad = afectado["tipo_ayuda"]
        horario = afectado["horario"]
        mensaje = afectado["mensaje_solicitud"]

        voluntarios_filtrados = [
            v for v in voluntarios
            if v["municipio"] == ciudad and necesidad in v["habilidades"]
            and v["disponibilidad"].get(horario, False)
        ]

        if voluntarios_filtrados:
            yield beam.pvalue.TaggedOutput(MATCHES_TAG, {
                "afectado": afectado,
                "voluntario": voluntarios_filtrados[0],
                "mensaje_solicitud": mensaje
            })
        else:
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

        # Aplicar transformación con TupleTags
        resultados = (
            afectados 
            | "Buscar Matches" >> beam.ParDo(MatchVoluntariosAfectados(), beam.pvalue.AsList(voluntarios))
            .with_outputs(MATCHES_TAG, NO_MATCH_TAG)
        )

        # Extraer las salidas de la transformación
        matches = resultados[MATCHES_TAG]
        sin_match = resultados[NO_MATCH_TAG]

        # Escribir las salidas en Pub/Sub
        matches | "Enviar Matches a Pub/Sub" >> beam.io.WriteToPubSub(TOPIC_MATCHES)
        sin_match | "Enviar No-Match a Pub/Sub" >> beam.io.WriteToPubSub(TOPIC_NO_MATCH)

if __name__ == "__main__":
    run()



