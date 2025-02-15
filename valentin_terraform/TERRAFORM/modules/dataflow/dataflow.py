import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
# Apache Beam ML Libraries (si son necesarias)
from apache_beam.ml.inference.base import ModelHandler, RunInference


from datetime import datetime
import logging
import json
import os

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)

def ParsePubSubMessages(message): 
    pubsub_message = message.decode('utf-8')
    return json.loads(pubsub_message)

def add_type(record, msg_type):
    record['type'] = msg_type
    return record

def increment_processed(record):
    record['processed'] = record.get('processed', 0) + 1
    return record

def key_by_match_fields(record):
    return ((record["city"], record["necessity"], record["disponibility"]), record)

def produce_matches(element):
    key, grouped = element
    afectados = grouped['affected']
    voluntarios = grouped['volunteer']
    for afectado in afectados:
        found_any = False
        for voluntario in voluntarios:
            found_any = True
            yield beam.pvalue.TaggedOutput('matched', {'afectado': afectado, 'volunteer': voluntario})
        if not found_any:
            yield beam.pvalue.TaggedOutput('non_matched_affected', afectado)
    if not afectados:
        for voluntario in voluntarios:
            yield beam.pvalue.TaggedOutput('non_matched_volunteer', voluntario)

def format_matched_for_bq(record):
    afectado = record['afectado']
    voluntario = record['volunteer']
    return {
        "city": afectado.get("city", ""),
        "necessity": afectado.get("necessity", ""),
        "disponibility": afectado.get("disponibility", ""),
        "affected_name": afectado.get("name", ""),
        "affected_phone": afectado.get("phone", ""),
        "volunteer_name": voluntario.get("name", ""),
        "volunteer_phone": voluntario.get("phone", ""),
        "affected_latitude": afectado.get("latitude", None),
        "affected_longitude": afectado.get("longitude", None)
    }

def format_unmatched_for_bq(record):
    return {
        "type": record.get("type", ""),
        "timestamp": record.get("timestamp", ""),
        "name": record.get("name", ""),
        "phone": record.get("phone", ""),
        "category": record.get("category", ""),
        "message": record.get("message", ""),
        "necessity": record.get("necessity", ""),
        "city": record.get("city", ""),
        "disponibility": record.get("disponibility", ""),
        "processed": record.get("processed", 0),
        "affected_latitude": record.get("latitude", None),
        "affected_longitude": record.get("longitude", None)
    }

def run():
    # Obtener parámetros desde variables de entorno
    project_id       = os.environ.get("PROJECT_ID")
    affected_sub     = os.environ.get("AFFECTED_SUB")
    volunteer_sub    = os.environ.get("VOLUNTEER_SUB")
    volunteer_topic  = os.environ.get("VOLUNTEER_TOPIC")
    affected_topic   = os.environ.get("AFFECTED_TOPIC")
    bq_dataset       = os.environ.get("BQ_DATASET")
    matched_table    = os.environ.get("MATCHED_TABLE", "matched")
    unmatched_table  = os.environ.get("UNMATCHED_TABLE", "unmatched")
    temp_location    = os.environ.get("TEMP_LOCATION")
    staging_location = os.environ.get("STAGING_LOCATION")
    region           = os.environ.get("REGION")


    # Validar que los parámetros requeridos estén definidos
    required = [project_id, affected_sub, volunteer_sub, volunteer_topic, affected_topic, bq_dataset, temp_location, staging_location, region]
    if any(v is None for v in required):
        logging.error("Faltan variables de entorno requeridas.")
        return

    # Configuración de PipelineOptions
    options = PipelineOptions(
        save_main_session=True,
        streaming=True,
        project=project_id,
        temp_location=temp_location,
        staging_location=staging_location,
        region=region
    )

    matched_table_id   = f"{project_id}:{bq_dataset}.{matched_table}"
    unmatched_table_id = f"{project_id}:{bq_dataset}.{unmatched_table}"

    # Esquemas para BigQuery
    matched_schema = {
        "fields": [
            {"name": "city", "type": "STRING", "mode": "REQUIRED"},
            {"name": "necessity", "type": "STRING", "mode": "REQUIRED"},
            {"name": "disponibility", "type": "STRING", "mode": "REQUIRED"},
            {"name": "affected_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "affected_phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "volunteer_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "volunteer_phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "affected_latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "affected_longitude", "type": "FLOAT", "mode": "NULLABLE"}
        ]
    }
    
    unmatched_schema = {
        "fields": [
            {"name": "type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "timestamp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "message", "type": "STRING", "mode": "NULLABLE"},
            {"name": "necessity", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "disponibility", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processed", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "affected_latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "affected_longitude", "type": "FLOAT", "mode": "NULLABLE"}
        ]
    }

    with beam.Pipeline(options=options) as p:
        # Lectura de mensajes de afectados
        affected_data = (
            p
            | "ReadAffected" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{affected_sub}")
            | "MarkTypeAffected" >> beam.Map(add_type, "affected")
            | "IncrementProcessedAffected" >> beam.Map(increment_processed)
            | "WindowAffected" >> beam.WindowInto(window.FixedWindows(60))
            | "KeyAffected" >> beam.Map(key_by_match_fields)
        )
    
        # Lectura de mensajes de voluntarios
        volunteer_data = (
            p
            | "ReadVolunteers" >> beam.io.ReadFromPubSub(subscription=f"projects/{project_id}/subscriptions/{volunteer_sub}")
            | "ParseVolunteers" >> beam.Map(ParsePubSubMessages)
            | "MarkTypeVolunteer" >> beam.Map(add_type, "volunteer")
            | "IncrementProcessedVolunteer" >> beam.Map(increment_processed)
            | "WindowVolunteers" >> beam.WindowInto(window.FixedWindows(60))
            | "KeyVolunteers" >> beam.Map(key_by_match_fields)
        )
    
        # Agrupar por clave (ciudad, necesidad, disponibility)
        grouped = (
            {
                'affected': affected_data,
                'volunteer': volunteer_data
            }
            | "CoGroupByKey" >> beam.CoGroupByKey()
        )
    
        results = (
            grouped
            | "ProduceMatches" >> beam.ParDo(produce_matches)
              .with_outputs('matched', 'non_matched_affected', 'non_matched_volunteer')
        )
    
        matched_pcoll = results['matched']
        unmatched_affected_pcoll = results['non_matched_affected']
        unmatched_volunteer_pcoll = results['non_matched_volunteer']
    
        # Escribir coincidencias en BigQuery
        (
            matched_pcoll
            | "FormatMatchedForBQ" >> beam.Map(format_matched_for_bq)
            | "WriteMatchedToBQ" >> WriteToBigQuery(
                table=matched_table_id,
                schema=matched_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )
    
        # Procesar no matcheados y re-publicar aquellos con processed < 7
        unmatched_affected_less_7 = unmatched_affected_pcoll | "FilterAff<7" >> beam.Filter(lambda x: x.get('processed', 0) < 7)
        unmatched_affected_ge_7   = unmatched_affected_pcoll | "FilterAff>=7" >> beam.Filter(lambda x: x.get('processed', 0) >= 7)
    
        unmatched_volunteer_less_7 = unmatched_volunteer_pcoll | "FilterVol<7" >> beam.Filter(lambda x: x.get('processed', 0) < 7)
        unmatched_volunteer_ge_7   = unmatched_volunteer_pcoll | "FilterVol>=7" >> beam.Filter(lambda x: x.get('processed', 0) >= 7)
    
        (
            unmatched_affected_less_7
            | "ReEncodeAff<7" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | "RePublishAff<7" >> beam.io.WriteToPubSub(topic=f"projects/{project_id}/topics/{affected_topic}")
        )
        (
            unmatched_volunteer_less_7
            | "ReEncodeVol<7" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | "RePublishVol<7" >> beam.io.WriteToPubSub(topic=f"projects/{project_id}/subscriptions/{volunteer_topic}")
        )
    
        unmatched_ge_7 = (
            (unmatched_affected_ge_7, unmatched_volunteer_ge_7)
            | "FlattenUnmatched>=7" >> beam.Flatten()
        )
    
        (
            unmatched_ge_7
            | "FormatUnmatchedForBQ" >> beam.Map(format_unmatched_for_bq)
            | "WriteUnmatchedToBQ" >> WriteToBigQuery(
                table=unmatched_table_id,
                schema=unmatched_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.info("The process started")
    run()
    logging.info("Process finished")
