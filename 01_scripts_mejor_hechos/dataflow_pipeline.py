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

    return msg['city'], msg

class BusinessLogicDoFn(beam.DoFn):
    def process(self, element):

        city, grouped_data = element
        affected_list = grouped_data.get('affected', []) if isinstance(grouped_data, dict) else []
        volunteer_list = grouped_data.get('volunteer', []) if isinstance(grouped_data, dict) else []
        
        matched_data = []
        unmatched_data = []
        
        for affected in affected_list:
            matched = False
            for volunteer in volunteer_list:
                if (affected['necessity'] == volunteer['necessity'] and
                    affected['disponibility'] == volunteer['disponibility'] and
                    affected['city'] == volunteer['city']):
                    matched_data.append({
                        "affected": affected,
                        "volunteer": volunteer,
                        "matched_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    })
                    matched = True
                    break
            if not matched:
                unmatched_data.append(affected)
        
        for volunteer in volunteer_list:
            if all(volunteer != match['volunteer'] for match in matched_data):
                unmatched_data.append(volunteer)
        
        yield beam.pvalue.TaggedOutput("matched_data", matched_data)
        yield beam.pvalue.TaggedOutput("unmatched_data", unmatched_data)





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

        # co Group by key 
        grouped_data= (
            affected_data, volunteer_data) | "merge PCollection" >> beam.CoGroupByKey()

        processed_data= (grouped_data
            |"Check the matched messages" >> beam.ParDo(BusinessLogicDoFn()).with_outputs("matched_data", "unmatched_data"))
        
        send_data= (
            
        )

        send_data.matched_data | "Publish matched messages" >> beam.io.WriteToPubSub(args.output_topic_matched)

        send_data.unmatched_data | "Publish unmatched messages" >> beam.io.WriteToPubSub(args.output_topic_non_matched)

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
    --job_name 'data-flow-pruebas-3' \
    --region 'europe-west1' \
    --temp_location 'gs://dataflow_bucket_dataproject_2425/tmp' \
    --staging_location 'gs://dataflow_bucket_dataproject_2425/stg' \
    --requirements_file 'requirements.txt'

    
    de aqui sacamos en claro que lee los mensajes y que le hace la window de 90 segundos
        '''