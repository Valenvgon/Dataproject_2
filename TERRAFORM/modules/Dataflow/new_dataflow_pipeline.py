import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition

# B. Apache Beam ML Libraries
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference

# C. Python Libraries
from datetime import datetime
import argparse
import logging
import json


def ParsePubSubMessages(message): 
    pubsub_message= message.decode('utf-8')

    msg = json.loads(pubsub_message)

    #logging.info("New message: %s", msg)

    return msg

def add_type(record, msg_type):
    """
    Asigna el campo "type" con el valor "affected" o "volunteer"
    para diferenciar el origen del mensaje.
    """
    record['type'] = msg_type
    return record

def increment_processed(record):
    """
    Incremente el campo 'processed'. Por defecto estará en 0,
    y cada vez que se lee el mensaje del Pub/Sub, se aumenta en 1.
    """
    record['processed'] = record.get('processed', 0) + 1
    return record

def key_by_match_fields(record):
    """
    Returns a tuple: (
        (city, necessity, disponibility),
        record
    )
    """
    return (
        (record["city"], record["necessity"], record["disponibility"]), 
        record
    )

def produce_matches(element):
    """
    Receives something like:
        element = ( key, { 'affected': [...], 'volunteer': [...] } )
    and we produce:
        - All matched (afectado, voluntario) pairs
        - All affected with NO volunteer
        - All volunteer with NO affected
    """
    key, grouped = element
    afectados = grouped['affected']
    voluntarios = grouped['volunteer']


    for afectado in afectados:
        found_any = False
        for voluntario in voluntarios:
            found_any = True
            yield beam.pvalue.TaggedOutput(
                'matched',
                {
                    'afectado': afectado,
                    'voluntario': voluntario
                }
            )
        if not found_any:
            # This afectado had zero volunteers
            yield beam.pvalue.TaggedOutput(
                'non_matched_affected',
                afectado
            )

    if not afectados:

        for voluntario in voluntarios:
            yield beam.pvalue.TaggedOutput('non_matched_volunteer', voluntario)

def format_matched_for_bq(record):
    """
    Del match, solo se pide:
      - city, necessity, disponibility
      - nombre y teléfono del afectado
      - nombre y teléfono del voluntario
    """
    afectado = record['afectado']
    voluntario = record['voluntario']

    return {
        "city": afectado.get("city", ""),
        "necessity": afectado.get("necessity", ""),
        "disponibility": afectado.get("disponibility", ""),
        "affected_name": afectado.get("name", ""),
        "affected_phone": afectado.get("phone", ""),
        "volunteer_name": voluntario.get("name", ""),
        "volunteer_phone": voluntario.get("phone", "")
    }

def format_unmatched_for_bq(record):
    """
        Para los no matcheados (cuando 'processed' >= 7), se desea una fila con:
         type, timestamp, name, phone, category, message,
        necessity, city, disponibility, processed
    """

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
        "processed": record.get("processed", 0)
    }

def run():
    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name, in this case data-project-2425')
    
    parser.add_argument(
                '--affected_sub',
                required=True,
                help='PubSub sub used for reading affected people. In this case the subscripcion will be: affected-sub')
    
    parser.add_argument(
                '--volunteer_sub',
                required=True,
                help='PubSub sub used for reading volunteer prople. In this case the subscripcion will be: volunteer-sub')
    
    parser.add_argument(
                '--volunteer_topic',
                required=True,
                help='PubSub Topic for storing data that has been processed and not matched. In this case the value will be: volunteer')
    
    parser.add_argument(
                '--affected_topic',
                required=True,
                help='PubSub Topic for storing data that has been processed and not matched. In this case the value will be: affected')
    
    parser.add_argument(
                '--bq_dataset',
                required=True, 
                help='Name of the BigQuery.')
    
    parser.add_argument(
                '--matched_table', 
                    default='matched', 
                    help='Name for the table of matches messages.')
    
    parser.add_argument(
                '--unmatched_table', 
                default='unmatched',  
                help='Name for the table of non-matches messages.')
    
    parser.add_argument(
                '--temp_location', 
                required=True,
                help= 'this will be: gs://dataflow_bucket_dataproject_2425/tmp')
    
    parser.add_argument(
                '--staging_location', 
                required=True,
                help='This will be: gs://dataflow_bucket_dataproject_2425/stg')
    
    
    args, pipeline_opts = parser.parse_known_args()

    options = PipelineOptions(pipeline_opts, 
        save_main_session= True, streaming= True, project= args.project_id)
    
    
    # Definimos los nombres completos de las tablas BQ (proyecto:dataset.tabla)
    matched_table_id = f"{args.project_id}:{args.bq_dataset}.{args.matched_table}"
    unmatched_table_id = f"{args.project_id}:{args.bq_dataset}.{args.unmatched_table}"

    # Definición de los esquemas de las tablas
    matched_schema = {
        "fields": [
            {"name": "city",            "type": "STRING", "mode": "REQUIRED"},
            {"name": "necessity",       "type": "STRING", "mode": "REQUIRED"},
            {"name": "disponibility",   "type": "STRING", "mode": "REQUIRED"},
            {"name": "affected_name",   "type": "STRING", "mode": "NULLABLE"},
            {"name": "affected_phone",  "type": "STRING", "mode": "NULLABLE"},
            {"name": "volunteer_name",  "type": "STRING", "mode": "NULLABLE"},
            {"name": "volunteer_phone", "type": "STRING", "mode": "NULLABLE"}
        ]
    }

    unmatched_schema = {
        "fields": [
            {"name": "type",           "type": "STRING",  "mode": "REQUIRED"}, 
            {"name": "timestamp",      "type": "STRING",  "mode": "NULLABLE"},
            {"name": "name",           "type": "STRING",  "mode": "NULLABLE"},
            {"name": "phone",          "type": "STRING",  "mode": "NULLABLE"},
            {"name": "category",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "message",        "type": "STRING",  "mode": "NULLABLE"},
            {"name": "necessity",      "type": "STRING",  "mode": "NULLABLE"},
            {"name": "city",           "type": "STRING",  "mode": "NULLABLE"},
            {"name": "disponibility",  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "processed",      "type": "INTEGER", "mode": "NULLABLE"}
        ]
    }

    with beam.Pipeline(options=options) as p:

        # 1) Lectura de Afectados: parsear JSON, setear type=affected, incrementar processed, agrupar
        affected_data = (
            p
            | "ReadAffected" >> beam.io.ReadFromPubSub(subscription=args.affected_sub)
            | "ParseAffected" >> beam.Map(ParsePubSubMessages)
            | "MarkTypeAffected" >> beam.Map(add_type, "affected")
            | "IncrementProcessedAffected" >> beam.Map(increment_processed)
            | "WindowAffected" >> beam.WindowInto(window.FixedWindows(60))
            | "KeyAffected" >> beam.Map(key_by_match_fields)
        )

        # 2) Lectura de Voluntarios: parsear JSON, setear type=volunteer, incrementar processed, agrupar
        volunteer_data = (
            p
            | "ReadVolunteers" >> beam.io.ReadFromPubSub(subscription=args.volunteer_sub)
            | "ParseVolunteers" >> beam.Map(ParsePubSubMessages)
            | "MarkTypeVolunteer" >> beam.Map(add_type, "volunteer")
            | "IncrementProcessedVolunteer" >> beam.Map(increment_processed)
            | "WindowVolunteers" >> beam.WindowInto(window.FixedWindows(60))
            | "KeyVolunteers" >> beam.Map(key_by_match_fields)
        )

        # 3) CoGroupByKey para emparejar (city, necessity, disponibility)
        grouped = (
            {
                'affected': affected_data,
                'volunteer': volunteer_data
            }
            | "CoGroupByKey" >> beam.CoGroupByKey()
        )

        # 4) produce_matches genera 3 salidas: matched, non_matched_affected, non_matched_volunteer
        results = (
            grouped
            | "ProduceMatches" >> beam.ParDo(produce_matches)
              .with_outputs('matched', 'non_matched_affected', 'non_matched_volunteer')
        )

        matched_pcoll = results['matched']
        unmatched_affected_pcoll = results['non_matched_affected']
        unmatched_volunteer_pcoll = results['non_matched_volunteer']

        # 5) Manejo de 'matched': se escribe directamente a la tabla BQ matcheada
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

        # 6) Manejo de NO matcheados:
        #    - Si processed < 7 => re-publicar a su tópico original
        #    - Si processed >= 7 => escribir a BQ "unmatched" con la estructura deseada

        # A) Afectados no matcheados
        unmatched_affected_less_7 = unmatched_affected_pcoll | "FilterAff<7" >> beam.Filter(lambda x: x.get('processed', 0) < 7)
        unmatched_affected_ge_7   = unmatched_affected_pcoll | "FilterAff>=7" >> beam.Filter(lambda x: x.get('processed', 0) >= 7)

        # B) Voluntarios no matcheados
        unmatched_volunteer_less_7 = unmatched_volunteer_pcoll | "FilterVol<7" >> beam.Filter(lambda x: x.get('processed', 0) < 7)
        unmatched_volunteer_ge_7   = unmatched_volunteer_pcoll | "FilterVol>=7" >> beam.Filter(lambda x: x.get('processed', 0) >= 7)

        # Re-publicar los que tienen processed < 7 al tópico original
        (
            unmatched_affected_less_7
            | "ReEncodeAff<7" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | "RePublishAff<7" >> beam.io.WriteToPubSub(topic=args.affected_topic)
        )
        (
            unmatched_volunteer_less_7
            | "ReEncodeVol<7" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | "RePublishVol<7" >> beam.io.WriteToPubSub(topic=args.volunteer_topic)
        )

        # Combinar todos los no matcheados >= 7 en un solo PCollection
        unmatched_ge_7 = (
            (unmatched_affected_ge_7, unmatched_volunteer_ge_7)
            | "FlattenUnmatched>=7" >> beam.Flatten()
        )

        # Guardar en BQ con la estructura deseada
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

    # Set Logs
    logging.basicConfig(level=logging.INFO)
    
    # Disable logs from apache_beam.utils.subprocess_server
    logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)

    logging.info("The process started")

    # Run Process
    run()

    '''

Till here i want to prove if the code is correct let's run it on dataflow  run pipeline in GCP: dataflow

        python new_dataflow_pipeline.py \
    --project_id 'data-project-2425' \
    --affected_sub 'projects/data-project-2425/subscriptions/affected-sub' \
    --volunteer_sub 'projects/data-project-2425/subscriptions/volunteer-sub' \
    --volunteer_topic 'projects/data-project-2425/topics/volunteer' \
    --affected_topic 'projects/data-project-2425/topics/affected' \
    --bq_dataset 'terreta_data' \
    --matched_table 'matched_table' \
    --unmatched_table 'non_matched_table' \
    --system_id 'vvercherg' \
    --runner DataflowRunner \
    --job_name 'data-flow-pruebas-1234-dataflow' \
    --region 'europe-west1' \
    --temp_location 'gs://dataflow_bucket_dataproject_2425/tmp' \
    --staging_location 'gs://dataflow_bucket_dataproject_2425/stg' \
    --requirements_file 'requirements.txt'

    
correrlo de forma local

    python new_dataflow_pipeline.py \
    --project_id 'data-project-2425' \
    --affected_sub 'projects/data-project-2425/subscriptions/affected-sub' \
    --volunteer_sub 'projects/data-project-2425/subscriptions/volunteer-sub' \
    --volunteer_topic 'projects/data-project-2425/topics/volunteer' \
    --affected_topic 'projects/data-project-2425/topics/affected' \
    --output_topic_non_matched 'projects/data-project-2425/topics/no-matched' \
    --output_topic_matched 'projects/data-project-2425/topics/matched' 
    


    topics :

    affected 
    volunteer
    matched
    no-matched 
    
    de aqui sacamos en claro que lee los mensajes y que le hace la window de 90 segundos
        '''