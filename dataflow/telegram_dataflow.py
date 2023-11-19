import argparse
import json
import logging
from datetime import datetime
import random

# from google.cloud import airplatform
from vertexai.language_models import TextEmbeddingModel
import apache_beam as beam
from apache_beam import DoFn, GroupByKey, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = 'big-d-project-404815'
TOPIC_NAME = 'telegram-topic'
TOPIC = f'projects/{PROJECT_ID}/topics/{TOPIC_NAME}'
BIGQUERY_DATASET = 'serving_layer'
BIGQUERY_TABLE = 'chatrooms'
LOCATION = 'europe-central2'
EMBEDDING_MODEL = 'textembedding-gecko@001'
MESSAGE_FIELDS = ['message_id', 'text', 'username', 'first_name', 'last_name', 'user_id', 'is_bot', 'channel_name', 'channel_id', 'timestamp', 'embedding']

def get_bigquery_schema(schema_path):
    with open(schema_path, 'r') as schema_file:
        schema_list = json.load(schema_file)
    return ','.join(f"{field['name']}:{field['type']}" for field in schema_list["schema"]['fields'])

schema_embeddings = get_bigquery_schema('schemas/embedding_schema.json')
schema_chatrooms = get_bigquery_schema('schemas/telegram_schema.json')

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
    
    def start_bundle(self):
        self.model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)

    def process(self, element):
        try:
            data = json.loads(element)
            text_to_embed = data.get('text', '')
            embedding = self.get_embedding(text_to_embed)
            data['embedding'] = embedding
            # yield json dump of data
            yield data

        except Exception as e:
            print(f"Error processing message: {e}")

    def get_embedding(self, text):
        embeddings = self.model.get_embeddings([text])
        for embedding in embeddings:
            vector = embedding.values
        return vector


class ExtractEmbeddings(DoFn):
    def process(self, element):
        if 'embedding' in element:
            message_id = element.get('message_id', '')
            embedding = element['embedding']
            yield {'message_id': message_id, 'embedding': embedding}


class ExtractChatrooms(DoFn):
    def process(self, element):
        # Exclude embeddings from the chatrooms table
        element.pop('embedding', None)
        yield element


def run(input_topic, output_path, bigquery_table, window_size=1.0, num_shards=3, pipeline_args=None):
    
    pipeline_options = PipelineOptions(
        pipeline_args, 
        streaming=True, 
        save_main_session=True,
        requirements_file='requirements.txt')
    
    process_pubsub = ProcessPubsubMessage()

    with Pipeline(options=pipeline_options) as pipeline:

        messages = (
            pipeline
            | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Decode PubSub message" >> beam.Map(lambda x: x.decode("utf-8"))
            | "Process message" >> beam.ParDo(process_pubsub)
            # filter invalid messages (not present field in MESSAGE_FIELDS)
            | "Filter out invalid message" >> beam.Filter(lambda x: all(field in x for field in MESSAGE_FIELDS) and x is not None)
        )

        gcs_output = (
            messages
            | "Batch messages" >> GroupMessagesByFixedWindows(window_size, num_shards)
            | "Write to GCS" >> ParDo(WriteToGCS(output_path))
        )

        # Write messages to chatrooms table in BigQuery (excluding embeddings)
        bq_output_chatrooms = (
            messages
            | "Extract chatrooms" >> ParDo(ExtractChatrooms())
            | "Write to BigQuery (chatrooms)" >> WriteToBigQuery(
                f'{PROJECT_ID}:{BIGQUERY_DATASET}.chatrooms',
                schema=schema_chatrooms,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

        # Extract embeddings and write to embeddings table in BigQuery
        bq_output_embeddings = (
            messages
            | "Extract embeddings" >> ParDo(ExtractEmbeddings())
            | "Write to BigQuery (embeddings)" >> WriteToBigQuery(
                f'{PROJECT_ID}:{BIGQUERY_DATASET}.embeddings',
                schema=schema_embeddings,
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
        default="projects/big-d-project-404815/topics/telegram-topic",
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
        default="gs://big-d-project-master-dataset/telegram",
    )
    parser.add_argument(
        "--bigquery_table",
        help="BigQuery table to write to.",
        default="chatrooms",
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
