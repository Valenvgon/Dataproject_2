import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics

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
                '--output_topic_non_matched',
                required=True,
                help='PubSub Topic for storing data of non matched messages. In this case it will be: no-matched')
        
    parser.add_argument(
                '--output_topic_matched',
                required=True,
                help='PubSub Topic for storing data of matched messages. In this case: matched')
    
    args, pipeline_opts = parser.parse_known_args()

    options = PipelineOptions(pipeline_opts, 
        save_main_session= True, streaming= True, project= args.project_id)
    
    with beam.Pipeline(options=options) as p:
        affected_data = (
            p
            | "Read affected data from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=args.affected_sub)
            | "Parse Affected" >> beam.Map(ParsePubSubMessages)
            | "Window Affected" >> beam.WindowInto(beam.window.FixedWindows(90))
            | "Key Affected" >> beam.Map(key_by_match_fields)
        )

        volunteer_data = (
            p
            | "Read volunteer data from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=args.volunteer_sub)
            | "Parse Volunteer" >> beam.Map(ParsePubSubMessages)
            | "Window Volunteer" >> beam.WindowInto(beam.window.FixedWindows(90))
            | "Key Volunteer" >> beam.Map(key_by_match_fields)
        )

        # Join them by CoGroupByKey
        grouped = (
            {
                'affected': affected_data,
                'volunteer': volunteer_data
            }
            | "CoGroupByKey" >> beam.CoGroupByKey()
        )

        # Produce separate PCollections for matched, non_matched_affected, non_matched_volunteer
        results = (
            grouped
            | "Match DoFn" >> beam.ParDo(produce_matches)
              .with_outputs('matched', 'non_matched_affected', 'non_matched_volunteer')
        )

        matched_pcoll = results['matched']
        unmatched_affected_pcoll = results['non_matched_affected']
        unmatched_volunteer_pcoll = results['non_matched_volunteer']

        # Now you can write them separately to Pub/Sub, or handle them differently:
        # 1) matched
        (matched_pcoll
         | 'codear matches'>>beam.Map(lambda x: json.dumps(x).encode("utf-8"))
         | 'write matched data' >> beam.io.WriteToPubSub(topic=args.output_topic_matched)
        )

        # 2) unmatched affected
        (unmatched_affected_pcoll
         | 'codear no matches aff'>>beam.Map(lambda x: json.dumps(x).encode("utf-8"))
         | 'write affected'>> beam.io.WriteToPubSub(topic=args.output_topic_non_matched)
        )

        # 3) unmatched volunteers
        # Possibly you want a separate Pub/Sub topic for them
        # or just the same topic—adjust as needed.
        (unmatched_volunteer_pcoll
         | 'codear no matches vol'>>beam.Map(lambda x: json.dumps(x).encode("utf-8"))
         | 'write volunteers'>>beam.io.WriteToPubSub(topic=args.output_topic_non_matched)
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
    


    topics :

    affected 
    volunteer
    matched
    no-matched 
    
    de aqui sacamos en claro que lee los mensajes y que le hace la window de 90 segundos
        '''