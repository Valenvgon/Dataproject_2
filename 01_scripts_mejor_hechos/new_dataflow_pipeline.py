import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
import json
import argparse
import logging

# Función para parsear mensajes de Pub/Sub
def ParsePubSubMessages(message): 
    pubsub_message = message.decode('utf-8')
    msg = json.loads(pubsub_message)
    return msg

# Función para generar clave de agrupación
def key_by_match_fields(record):
    return (
        (record["city"], record["necessity"], record["disponibility"]), 
        record
    )

# Función para procesar los matches y reprocesar afectados sin match
def produce_matches(element):
    key, grouped = element
    afectados = grouped['affected']
    voluntarios = grouped['volunteer']

    for afectado in afectados:
        found_any = False
        for voluntario in voluntarios:
            found_any = True
            yield beam.pvalue.TaggedOutput('matched', {'afectado': afectado, 'voluntario': voluntario})
        if not found_any:
            afectado['reprocess_attempts'] = afectado.get('reprocess_attempts', 0) + 1
            if afectado['reprocess_attempts'] < 7:
                yield beam.pvalue.TaggedOutput('reprocess_affected', afectado)
            else:
                yield beam.pvalue.TaggedOutput('bigquery_unmatched', afectado)

    if not afectados:
        for voluntario in voluntarios:
            yield beam.pvalue.TaggedOutput('non_matched_volunteer', voluntario)

# Función para formatear datos antes de escribir en BigQuery
def format_for_bigquery(element):
    return {
        "affected_id": element.get("affected_id", ""),
        "timestamp": element.get("timestamp", ""),
        "name": element.get("name", ""),
        "phone": element.get("phone", ""),
        "category": element.get("category", ""),
        "message": element.get("message", ""),
        "necessity": element.get("necessity", ""),
        "city": element.get("city", ""),
        "disponibility": element.get("disponibility", ""),
        "latitude": element.get("latitude", 0.0),
        "longitude": element.get("longitude", 0.0),
        "reprocess_attempts": element.get("reprocess_attempts", 0)
    }

def run():
    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument('--project_id', required=True, help='GCP Project ID')
    parser.add_argument('--affected_sub', required=True, help='Pub/Sub subscription for affected messages')
    parser.add_argument('--affected_topic', required=True, help='Pub/Sub topic for reprocessing affected messages')
    parser.add_argument('--volunteer_sub', required=True, help='Pub/Sub subscription for volunteer messages')
    parser.add_argument('--output_topic_non_matched', required=True, help='Pub/Sub topic for non-matched messages')
    parser.add_argument('--output_topic_matched', required=True, help='Pub/Sub topic for matched messages')
    parser.add_argument('--bq_dataset', required=True, help='BigQuery dataset')
    parser.add_argument('--bq_table', required=True, help='BigQuery table for unmatched affected messages')

    args, pipeline_opts = parser.parse_known_args()

    options = PipelineOptions(
        pipeline_opts, save_main_session=True, streaming=True, project=args.project_id
    )

    with beam.Pipeline(options=options) as p:
        affected_data = (
            p
            | "Read affected from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=args.affected_sub)
            | "Parse Affected" >> beam.Map(ParsePubSubMessages)
            | "Window Affected" >> beam.WindowInto(beam.window.FixedWindows(90))
            | "Key Affected" >> beam.Map(key_by_match_fields)
        )

        volunteer_data = (
            p
            | "Read volunteer from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=args.volunteer_sub)
            | "Parse Volunteer" >> beam.Map(ParsePubSubMessages)
            | "Window Volunteer" >> beam.WindowInto(beam.window.FixedWindows(90))
            | "Key Volunteer" >> beam.Map(key_by_match_fields)
        )

        grouped = (
            {"affected": affected_data, "volunteer": volunteer_data}
            | "CoGroupByKey" >> beam.CoGroupByKey()
        )

        results = (
            grouped
            | "Process Matches" >> beam.ParDo(produce_matches)
              .with_outputs('matched', 'reprocess_affected', 'bigquery_unmatched', 'non_matched_volunteer')
        )

        # Escribir matches en Pub/Sub
        (
            results['matched']
            | "Encode Matched" >> beam.Map(lambda x: json.dumps(x).encode("utf-8"))
            | "Write Matched to Pub/Sub" >> beam.io.WriteToPubSub(topic=args.output_topic_matched)
        )

        # Reprocesar afectados sin match
        (
            results['reprocess_affected']
            | "Encode Reprocessed Affected" >> beam.Map(lambda x: json.dumps(x).encode("utf-8"))
            | "Reprocess Affected" >> beam.io.WriteToPubSub(topic=args.affected_topic)
        )

        # Enviar afectados que no encontraron match a BigQuery
        (
            results['bigquery_unmatched']
            | "Format for BigQuery" >> beam.Map(format_for_bigquery)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                f"{args.project_id}:{args.bq_dataset}.{args.bq_table}",
                schema="affected_id:STRING, timestamp:STRING, name:STRING, phone:STRING, category:STRING, "
                       "message:STRING, necessity:STRING, city:STRING, disponibility:STRING, "
                       "latitude:FLOAT, longitude:FLOAT, reprocess_attempts:INTEGER",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)
    logging.info("Starting Dataflow Pipeline")
    run()



    '''

Till here i want to prove if the code is correct let's run it on dataflow  run pipeline in GCP: dataflow

        python new_dataflow_pipeline.py \
    --project_id 'data-project-2425' \
    --affected_sub 'projects/data-project-2425/subscriptions/affected-sub' \
    --volunteer_sub 'projects/data-project-2425/subscriptions/volunteer-sub' \
    --output_topic_non_matched 'projects/data-project-2425/topics/no-matched' \
    --output_topic_matched 'projects/data-project-2425/topics/matched' \
    --system_id 'vvercherg' \
    --runner DataflowRunner \
    --job_name 'data-flow-pruebas-999' \
    --region 'europe-west1' \
    --temp_location 'gs://dataflow_bucket_dataproject_2425/tmp' \
    --staging_location 'gs://dataflow_bucket_dataproject_2425/stg' \
    --requirements_file 'requirements.txt'

    
correrlo de forma local

    python new_dataflow_pipeline.py \
    --project_id 'data-project-2425' \
    --affected_sub 'projects/data-project-2425/subscriptions/affected-sub' \
    --volunteer_sub 'projects/data-project-2425/subscriptions/volunteer-sub' \
    --output_topic_non_matched 'projects/data-project-2425/topics/no-matched' \
    --output_topic_matched 'projects/data-project-2425/topics/matched' 

    * Para correrlo en local, a mí solo me funciona con este comando (Lara):

    python new_dataflow_pipeline.py \
    --project_id data-project-2425 \
    --affected_sub projects/data-project-2425/subscriptions/affected-sub \
    --volunteer_sub projects/data-project-2425/subscriptions/volunteer-sub \
    --output_topic_non_matched projects/data-project-2425/topics/no-matched \
    --output_topic_matched projects/data-project-2425/topics/matched \
    --runner DataflowRunner \
    --region europe-southwest1 \
    --temp_location gs://dataflow_bucket_dataproject_2425/tmp \
    --staging_location gs://dataflow_bucket_dataproject_2425/stg

    *Una vez tenemos creada la tabla de peticiones_no_atendidas en BigQuery, corremos el scripy con este comando (Lara):

   python new_dataflow_pipeline.py \
    --project_id data-project-2425 \
    --affected_sub projects/data-project-2425/subscriptions/affected-sub \
    --affected_topic projects/data-project-2425/topics/affected \
    --volunteer_sub projects/data-project-2425/subscriptions/volunteer-sub \
    --output_topic_non_matched projects/data-project-2425/topics/no-matched \
    --output_topic_matched projects/data-project-2425/topics/matched \
    --bq_dataset peticiones_no_atendidas_dataset \
    --bq_table peticiones_no_atendidas \
    --runner DataflowRunner \
    --region europe-southwest1 \
    --temp_location gs://dataflow_bucket_dataproject_2425/tmp \
    --staging_location gs://dataflow_bucket_dataproject_2425/stg





    


    topics :

    affected 
    volunteer
    matched
    no-matched 
    
    de aqui sacamos en claro que lee los mensajes y que le hace la window de 90 segundos
        '''