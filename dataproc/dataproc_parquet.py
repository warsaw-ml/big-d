import pandas as pd
from google.cloud import storage
import json
from datetime import datetime
import os
import io
import pyarrow as pa
import pyarrow.parquet as pq

key_file_path = "big-d-project-404815-9c427ac42549.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path

project_id = 'big-d-project-404815'
bucket_name = 'big-d-project-master-dataset'
output_file_prefix = 'batch'
output_file_suffix = '.parquet'

storage_client = storage.Client()

def process_and_save_partition(bucket_name, file_paths, source_dir):
    is_bitcoin = False
    if source_dir == 'bitcoin':
        is_bitcoin =True
    if is_bitcoin== False:
        bitcoin_data_list = []
        bitcoin_blobs = storage_client.bucket(bucket_name).list_blobs(prefix=f"{'bitcoin'}/")
        bitcoin_file_paths = [blob.name for blob in bitcoin_blobs if blob.name.endswith('.json')]
        for file_path in bitcoin_file_paths:
            blob = storage_client.bucket(bucket_name).blob(file_path)
            json_data_str = blob.download_as_text()
            json_data_list = json.loads(json_data_str)
            bitcoin_data_list.extend(json_data_list)

    data_list = []

    for file_path in file_paths:
        blob = storage_client.bucket(bucket_name).blob(file_path)
        json_data_str = blob.download_as_text()
        json_data_list = json.loads(json_data_str)
        data_list.extend(json_data_list)

    if data_list:
        telegram_data = pd.DataFrame(data_list)
        telegram_data['timestamp'] = pd.to_datetime(telegram_data['timestamp'])
        telegram_data = telegram_data.sort_values(by='timestamp')

        if is_bitcoin== False:
            telegram_data['is_bot'] = telegram_data['is_bot'].apply(lambda x: 1 if x == 'true' else 0)
            telegram_data = telegram_data.drop(columns=['first_name', 'last_name'])
            bitcoin_data = pd.DataFrame(bitcoin_data_list)
            bitcoin_data['timestamp'] = pd.to_datetime(bitcoin_data['timestamp'])
            bitcoin_data = bitcoin_data.sort_values(by='timestamp')
            data = pd.merge_asof(telegram_data, bitcoin_data, on='timestamp', direction='nearest')
        else:
            data = telegram_data

        data['year'] = data['timestamp'].dt.year
        data['month'] = data['timestamp'].dt.month
        data['day'] = data['timestamp'].dt.day
        data['hour'] = data['timestamp'].dt.hour
        data = data.fillna(method='ffill')
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)

        for _, partition_data in data.groupby(['year', 'month', 'day', 'hour']):
            partition_name = f"parquet/{output_file_prefix}_{partition_data['year'].iloc[0]}_{partition_data['month'].iloc[0]:02d}_{partition_data['day'].iloc[0]:02d}_{partition_data['hour'].iloc[0]:02d}{output_file_suffix}"

            output_buffer = io.BytesIO()
            partition_data.drop(columns=['year', 'month', 'day', 'hour'], inplace=True)
            table = pa.Table.from_pandas(partition_data)
            pq.write_table(table, output_buffer, compression='snappy')

            output_buffer.seek(0)
            parquet_table = pq.read_table(output_buffer)
            parquet_df = parquet_table.to_pandas()
            print(parquet_df.head(5))

            output_blob = storage_client.bucket(bucket_name).blob(partition_name)
            output_buffer.seek(0)
            output_blob.upload_from_file(output_buffer, content_type='application/octet-stream')

        print(f"Processed data from {file_paths} saved to Google Cloud Storage with partitioning.")
    else:
        print(f"No data in {file_paths} to process.")

source_directories = ['telegram', "telegram-batch", "bitcoin"]

for source_dir in source_directories:
    blobs = storage_client.bucket(bucket_name).list_blobs(prefix=f"{source_dir}/")
    file_paths = [blob.name for blob in blobs if blob.name.endswith('.json')]
    process_and_save_partition(bucket_name, file_paths, source_dir)
