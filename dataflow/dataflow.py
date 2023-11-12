import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import json


PROJECT_ID = 'big-d-project-404815'
TOPIC_NAME = 'telegram-topic'
TOPIC = f'projects/{PROJECT_ID}/topics/{TOPIC_NAME}'
BIGQUERY_DATASET = 'telegram'
BIGQUERY_TABLE = 'chatroom'


# Pipeline setup
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = PROJECT_ID
google_cloud_options.job_name = 'pubsub-to-bigquery'
google_cloud_options.staging_location = "gs://big-d-project-temp-bucket/staging"
google_cloud_options.temp_location = "gs://big-d-project-temp-bucket/temp"


options.view_as(beam.options.pipeline_options.StandardOptions).streaming = True

p = beam.Pipeline(options=options)

# Read BigQuery schema
def get_bigquery_schema(schema_path):
    with open(schema_path, 'r') as schema_file:
        schema_list = json.load(schema_file)
    return ','.join(f"{field['name']}:{field['type']}" for field in schema_list)

schema = get_bigquery_schema('schema.json')

# Process Pub/Sub
def process_pubsub_message(message):
    try:
        data = eval(message) if isinstance(message, str) else message.data
        return data
    except Exception as e:
        print(f"Error processing message: {e}")
        return None
    

# Read from Pub/Sub
messages = (
    p
    | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=TOPIC)
    | 'Decode Pub/Sub message' >> beam.Map(lambda x: x.decode('utf-8'))
    | 'Process Pub/Sub message' >> beam.Map(process_pubsub_message)
    | 'Filter out invalid messages' >> beam.Filter(lambda x: x is not None)
)

# Write to BigQuery
(messages
 | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
        '{}:{}.{}'.format(PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE),
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )
)

# Run the pipeline
result = p.run()
result.wait_until_finish()
