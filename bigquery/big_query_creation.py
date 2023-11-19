from google.cloud import bigquery

# Replace these values with your project ID, dataset name, and table name
project_id = "big-d-project-404815"
dataset_name = 'serving_layer'
table_names = ['chatrooms', 'embeddings', 'bitcoin']
# Create BigQuery client
client = bigquery.Client(project=project_id)

# Create dataset
dataset_ref = client.dataset(dataset_name)

# if dataset not exists
if not client.get_dataset(dataset_ref,):
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset)

# Define table schema
chatrooms_schema = [
    bigquery.SchemaField("message_id", "STRING"),
    bigquery.SchemaField("text", "STRING"),
    bigquery.SchemaField("username", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("user_id", "STRING"),
    bigquery.SchemaField("is_bot", "BOOLEAN"),
    bigquery.SchemaField("channel_name", "STRING"),
    bigquery.SchemaField("channel_id", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
]

embeddings_schema = [
    bigquery.SchemaField("message_id", "STRING"),
    bigquery.SchemaField("embedding", "FLOAT", mode="REPEATED")
]

bitcoin_schema = [
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("price", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP")
    # Add more fields as needed
]

schemas = [chatrooms_schema, embeddings_schema, bitcoin_schema]

for i, table_name in enumerate(table_names):
    try:
        # Attempt to get the table, will raise NotFound if not exists
        table_ref = dataset_ref.table(table_name)
        client.get_table(table_ref)
        print(f"Table {table_name} already exists.")
    except Exception as e:
        # Table not found, create it
        print(f"Creating table {table_name}.")
        table_ref = dataset_ref.table(table_name)
        table = bigquery.Table(table_ref, schema=schemas[i])
        table = client.create_table(table)
