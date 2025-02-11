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

beam.options.pipeline_options.PipelineOptions.allow_non_parallel_instruction_output = True
DataflowRunner.__test__ = False


def ParsePubSubMessages(message): 
    pubsub_message= message.decode('utf-8')

    msg = json.loads(pubsub_message)

    #logging.info("New message: %s", msg)

    return msg

class MatchVoluntariosAfectados(beam.DoFn):

    def process(self, affected_data, volunteer_data):

        import apache_beam as beam

        city = affected_data["city"]
        necessity = affected_data["necessity"]
        disponibility = affected_data["disponibility"]

        volunteer_filtered = [
            v for v in volunteer_data
            if v["city"] == city and necessity == v["necessity"] and disponibility == v["disponibility"]
        ]

        if volunteer_filtered:
            match = {
                "afectado": affected_data,
                "voluntario": volunteer_filtered[0]
            }

            logging.info(f"MATCH encontrado: {match}")
            yield beam.pvalue.TaggedOutput('matched_data', match)
        
        else:
            logging.info(f"NO MATCH: {affected_data}")
            yield beam.pvalue.TaggedOutput('non_matched_data', affected_data)

            logging.info(f"NO MATCH: {volunteer_filtered}")
            yield beam.pvalue.TaggedOutput('non_matched_data', volunteer_data)

            


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
    
    with beam.Pipeline(argv= pipeline_opts, options=options) as p:

        affected_data=(
            p
                |"Read affected data from Pub/Sub" >> beam.io.ReadFromPubSub(subscription= args.affected_sub)
                |"Parse Json battery messages" >> beam.Map(ParsePubSubMessages)
                |" Fixed window for Affected data" >>beam.WindowInto(beam.window.FixedWindows(90))
        )

        volunteer_data=(
            p 
                |"Read volunteer data from Pub/Sub">> beam.io.ReadFromPubSub(subscription=args.volunteer_sub)
                |"Parse Json from Volunteer messages">> beam.Map(ParsePubSubMessages)
                |"Fixed window for Volunteer data" >> beam.WindowInto(beam.window.FixedWindows(90))
        )

        results = (
            affected_data 
            | "Buscar Matches" >> beam.ParDo(MatchVoluntariosAfectados(), beam.pvalue.AsList(volunteer_data))
            .with_outputs('matched_data', 'non_matched_data')
        )

        matches = results['matched_data'] 
        sin_match = results['non_matched_data']

        # Enviar Matches a Pub/Sub
        matches | "Formato Matches JSON" >> beam.Map(lambda x: json.dumps(x).encode("utf-8")) \
                | "Enviar Matches a Pub/Sub" >> beam.io.WriteToPubSub(topic=args.output_topic_matched)

        # Enviar No Matches a Pub/Sub
        sin_match | "Formato No-Matches JSON" >> beam.Map(lambda x: json.dumps(x).encode("utf-8")) \
                  | "Enviar No-Match a Pub/Sub" >> beam.io.WriteToPubSub(topic= args.output_topic_non_matched)

        

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

        python dataflow_pipeline.py \
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

    python dataflow_pipeline.py \
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