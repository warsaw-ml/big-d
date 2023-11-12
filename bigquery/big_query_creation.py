from google.cloud import bigquery

# Replace these values with your project ID, dataset name, and table name
project_id = "big-d-project-404815"
dataset_name = 'telegram'
table_name = 'chatroom'

# Create BigQuery client
client = bigquery.Client(project=project_id)

# Create dataset
dataset_ref = client.dataset(dataset_name)
dataset = bigquery.Dataset(dataset_ref)
dataset = client.create_dataset(dataset)

# Define table schema
schema = [
    bigquery.SchemaField("text", "STRING"),
    bigquery.SchemaField("username", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("is_bot", "BOOLEAN"),
    bigquery.SchemaField("channel_name", "STRING"),
    bigquery.SchemaField("channel_id", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP")
    # Add more fields as needed
]

# Create table
table_ref = dataset_ref.table(table_name)
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)
