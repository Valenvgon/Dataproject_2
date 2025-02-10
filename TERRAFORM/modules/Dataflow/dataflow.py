import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import pubsub_v1
import json

PROJECT_ID = "data-project-2425"
AFFECTED_SUBSCRIPTION = "projects/{}/subscriptions/afectados-sub".format(PROJECT_ID)
VOLUNTEER_SUBSCRIPTION = "projects/{}/subscriptions/voluntarios-sub".format(PROJECT_ID)
MATCHED_TABLE = "{}.help_matching.matched_pairs".format(PROJECT_ID)
UNMATCHED_TABLE = "{}.help_matching.unmatched_requests".format(PROJECT_ID)

class ParsePubSubMessage(beam.DoFn):
    def process(self, message):
        decoded_message = json.loads(message.decode('utf-8'))
        return [decoded_message]

class MatchHelpRequests(beam.DoFn):
    def process(self, element, volunteer_dict):
        city = element['city']
        necessity = element['necessity']
        disponibility = element['disponibility']
        key = (city, necessity, disponibility)
        
        if key in volunteer_dict:
            volunteer = volunteer_dict[key]
            return [{"affected": element, "volunteer": volunteer}]
        return [{"unmatched": element}]

class FormatForBigQuery(beam.DoFn):
    def process(self, matched_pair):
        if "volunteer" in matched_pair:
            return [{
                'affected_id': matched_pair['affected']['affected_id'],
                'affected_name': matched_pair['affected']['name'],
                'affected_phone': matched_pair['affected']['phone'],
                'affected_city': matched_pair['affected']['city'],
                'necessity': matched_pair['affected']['necessity'],
                'disponibility': matched_pair['affected']['disponibility'],
                'volunteer_id': matched_pair['volunteer']['volunteer_id'],
                'volunteer_name': matched_pair['volunteer']['name'],
                'volunteer_phone': matched_pair['volunteer']['phone'],
                'volunteer_city': matched_pair['volunteer']['city'],
            }]
        else:
            return [{
                'affected_id': matched_pair['unmatched']['affected_id'],
                'affected_name': matched_pair['unmatched']['name'],
                'affected_phone': matched_pair['unmatched']['phone'],
                'affected_city': matched_pair['unmatched']['city'],
                'necessity': matched_pair['unmatched']['necessity'],
                'disponibility': matched_pair['unmatched']['disponibility'],
            }]

options = PipelineOptions(
    streaming=True,
    project=PROJECT_ID,
    region='europe-west1'
)
options.view_as(StandardOptions).streaming = True

with beam.Pipeline(options=options) as pipeline:
    affected = (
        pipeline
        | "Read Affected from PubSub" >> beam.io.ReadFromPubSub(subscription=AFFECTED_SUBSCRIPTION)
        | "Parse Affected Messages" >> beam.ParDo(ParsePubSubMessage())
    )

    volunteers = (
        pipeline
        | "Read Volunteers from PubSub" >> beam.io.ReadFromPubSub(subscription=VOLUNTEER_SUBSCRIPTION)
        | "Parse Volunteer Messages" >> beam.ParDo(ParsePubSubMessage())
        | "Create Volunteer Dict" >> beam.Map(lambda v: ((v['city'], v['necessity'], v['disponibility']), v))
    )

    matched_or_unmatched = (
        affected
        | "Match Affected with Volunteers" >> beam.ParDo(MatchHelpRequests(), beam.pvalue.AsDict(volunteers))
    )

    matched = matched_or_unmatched | "Filter Matched" >> beam.Filter(lambda x: "volunteer" in x)
    unmatched = matched_or_unmatched | "Filter Unmatched" >> beam.Filter(lambda x: "unmatched" in x)

    matched | "Format Matched for BigQuery" >> beam.ParDo(FormatForBigQuery()) | "Write Matched to BigQuery" >> beam.io.WriteToBigQuery(
        MATCHED_TABLE,
        schema="affected_id:STRING, affected_name:STRING, affected_phone:STRING, affected_city:STRING, necessity:STRING, disponibility:STRING, volunteer_id:STRING, volunteer_name:STRING, volunteer_phone:STRING, volunteer_city:STRING",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )

    unmatched | "Format Unmatched for BigQuery" >> beam.ParDo(FormatForBigQuery()) | "Write Unmatched to BigQuery" >> beam.io.WriteToBigQuery(
        UNMATCHED_TABLE,
        schema="affected_id:STRING, affected_name:STRING, affected_phone:STRING, affected_city:STRING, necessity:STRING, disponibility:STRING",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )

'''

python dataflow.py \
    --runner DataflowRunner \
    --project data-project-2425 \
    --region europe-west1 \
    --temp_location gs://BUCKET_TEMP/temp \
    --staging_location gs://BUCKET_TEMP/staging \
    --streaming


'''