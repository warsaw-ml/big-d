import argparse
import json
import logging
from datetime import datetime
import random
import pytz

import apache_beam as beam
from apache_beam import DoFn, GroupByKey, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = 'big-d-project-404815'
TOPIC_NAME = 'bitcoin-topic'
TOPIC = f'projects/{PROJECT_ID}/topics/{TOPIC_NAME}'
BIGQUERY_DATASET = 'telegram'
BIGQUERY_TABLE = 'chatrooms'
LOCATION = 'europe-central2'


def get_bigquery_schema(schema_path):
    with open(schema_path, 'r') as schema_file:
        schema_list = json.load(schema_file)
    return ','.join(f"{field['name']}:{field['type']}" for field in schema_list["schema"]['fields'])

schema = get_bigquery_schema('schema.json')


class GroupMessagesByFixedWindows(PTransform):
    def __init__(self, window_size, num_shards=3):
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            | "Window into fixed intervals" >> WindowInto(beam.transforms.window.FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            | "Group by key" >> GroupByKey()
        )

class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        yield (
            element,
            datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f"),
        )

class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=DoFn.WindowParam):
        ts_format = "%Y-%m-%d %H:%M:%S"
        window_start = window.start.to_utc_datetime().strftime(ts_format)

        # Extract date and hour from the window start time
        date_str, time_str = window_start.split(" ")
        hour_str, minute_str, _ = time_str.split(":")
        # Combine date and hour to form the folder name
        folder_name = f"{date_str}/{hour_str}"

        shard_id, batch = key_value
        filename = f"{self.output_path}/{folder_name}/{minute_str}_{shard_id}.json"

        # Extract message bodies from the batch
        messages = [message_body for message_body, _ in batch]

        with beam.io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            # Write JSON containing the list of messages
            f.write(json.dumps(messages).encode())


class ProcessPubsubMessage(DoFn):

    def process(self, element):
        element = json.loads(element)
        element['timestamp'] = datetime.now(pytz.utc).isoformat()
        return [json.dumps(element)]


def run(input_topic, output_path, bigquery_table, window_size=1.0, num_shards=3, pipeline_args=None):
    pipeline_options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)
    process_pubsub = ProcessPubsubMessage()

    with Pipeline(options=pipeline_options) as pipeline:

        messages = (
            pipeline
            | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Decode PubSub message" >> beam.Map(lambda x: x.decode("utf-8"))
            | "Process message" >> beam.ParDo(process_pubsub)
            | "Filter out invalid message" >> beam.Filter(lambda x: x is not None)
        )
    
        gcs_output = (
                    messages
                    | "Batch messages" >> GroupMessagesByFixedWindows(window_size, num_shards)
                    | "Write to GCS" >> ParDo(WriteToGCS(output_path))
        )

        bq_output = (
            messages
            | "Write to BigQuery" >> WriteToBigQuery(
                f'{PROJECT_ID}:{BIGQUERY_DATASET}.{bigquery_table}',
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        
        )

if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
        default="projects/big-d-project-404815/topics/bitcoin-topic",
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in minutes.",
    )
    parser.add_argument(
        "--output_path",
        help="Path of the output GCS file including the prefix.",
        default="gs://big-d-project-master-dataset/bitcoin",
    )
    parser.add_argument(
        "--bigquery_table",
        help="BigQuery table to write to.",
        default="chatroom",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=3,
        help="Number of shards to use when writing windowed elements to GCS.",
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        known_args.output_path,
        known_args.bigquery_table,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args,
    )