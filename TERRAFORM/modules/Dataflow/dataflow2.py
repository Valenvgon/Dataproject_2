''' Este es el script que vamos a utilizar para hacer el Dataflow'''

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

'''helpfull functions'''

def ParsePubSubMessages(message):
    '''
    This def() allow us to decode the message
    '''
    pubsub_message = message.decode('utf-8')

    msg= json.loads(pubsub_message)

    logging.info("New message decoded: %s", msg)

    return msg

def run(): 
    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.')) 

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--afectados',
                required=True,
                help='PubSub subscription used for gente afectada.')
    
    parser.add_argument(
                '--voluntarios',
                required=True,
                help='PubSub subscription used for gente voluntaria.')
    
    parser.add_argument(
                '--bigquery',
                required=False,
                default="nombre de la tabla de bigquery para los matches",
                help='The Firestore collection where the telemetry data will be stored.')

    parser.add_argument(
                '--output_topic',
                required=False,
                help='PubSub Topic for sending push notifications.')
    
    args, pipeline_opts = parser.parse_known_args()

    ''' Pipe lines'''

    #A. Pipe lines Options

    options = PipelineOptions(pipeline_opts, 
        save_main_session= True, streaming= True, project= args.project_id)
    
    #B. Pipe line Object 

    with beam.Pipeline(argv=pipeline_opts, options=options) as p:

        '''
        aqui estan las pipe lines que vamos a utilizar con el dataflow
        '''
    
    volunteer_data = (
        p 
            |'Read the messages of volunteer people' >> beam.io.ReadFromPubSub(subscription= args.voluntarios)
            |'Parse Json with the message for volunteer' >> beam.Map(ParsePubSubMessages)
            |'Fixed window for messages for colunteer' >> beam.WindowInto(beam.window.FixedWindows(90))
    )

    afectados_data = (
        p 
            |'Read the messages of afected people' >> beam.io.ReadFromPubSub(subscription= args.afectados)
            |'Parse Json with the message for afectados' >> beam.Map(ParsePubSubMessages)
            |'Fixed window for messages for afectados' >> beam.WindowInto(beam.window.FixedWindows(90))
    )

    logging.info('message read: %s', volunteer_data)
    logging.info('message read: %s', afectados_data)

if __name__ == '__main__':

    logging.basicConfig(level= logging.INFO)
    
    logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)

    logging.info("The process started")

    # Run Process
    run()