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

    logging.info("New message: %s", msg)

    return msg

def run(): 
    
    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name, in this case data-project-2425')
    
    parser.add_argument(
                '--affected-subscription',
                required=True,
                help='PubSub subscription used for reading affected_people.')
    
    parser.add_argument(
                '--volunteer-subscription',
                required=True,
                help='PubSub subscription used for reading driving telemetry data.')
    
    parser.add_argument(
                '--environment_telemetry_subscription',
                required=True,
                help='PubSub subscription used for reading environment telemetry data.')
    
    parser.add_argument(
                '--firestore_collection',
                required=True,
                default="vehicle_telemetry_data",
                help='The Firestore collection where the telemetry data will be stored.')
    
    parser.add_argument(
                '--output_topic',
                required=True,
                help='PubSub Topic for sending push notifications.')
