import pandas as pd
from google.cloud import storage
import json
from datetime import datetime
import os
import io
import pyarrow as pa
import pyarrow.parquet as pq

project_id = 'big-d-project-404815'
bucket_name = 'big-d-project-master-dataset'
output_file_prefix = 'batch'
output_file_suffix = '.parquet'

storage_client = storage.Client()

def process_and_save_partition(bucket_name, file_paths):
    data_list = []

    for file_path in file_paths:
        blob = storage_client.bucket(bucket_name).blob(file_path)
        json_data_str = blob.download_as_text()

        json_data_list = json.loads(json_data_str)

        data_list.extend(json_data_list)

    if data_list:
        data = pd.DataFrame(data_list)
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        data['year'] = data['timestamp'].dt.year
        data['month'] = data['timestamp'].dt.month
        data['day'] = data['timestamp'].dt.day
        data['hour'] = data['timestamp'].dt.hour

        for _, partition_data in data.groupby(['year', 'month', 'day', 'hour']):
            partition_name = f"parquet/{output_file_prefix}_{partition_data['year'].iloc[0]}_{partition_data['month'].iloc[0]:02d}_{partition_data['day'].iloc[0]:02d}_{partition_data['hour'].iloc[0]:02d}{output_file_suffix}"

            output_buffer = io.BytesIO()
            partition_data.drop(columns=['year', 'month', 'day', 'hour'], inplace=True)
            table = pa.Table.from_pandas(partition_data)
            pq.write_table(table, output_buffer, compression='snappy')

            output_blob = storage_client.bucket(bucket_name).blob(partition_name)
            output_buffer.seek(0)
            output_blob.upload_from_file(output_buffer, content_type='application/octet-stream')

        print(f"Processed data from {file_paths} saved to Google Cloud Storage with partitioning.")
    else:
        print(f"No data in {file_paths} to process.")

source_directories = ['telegram', 'bitcoin', "telegram-batch"]

for source_dir in source_directories:
    blobs = storage_client.bucket(bucket_name).list_blobs(prefix=f"{source_dir}/")
    file_paths = [blob.name for blob in blobs if blob.name.endswith('.json')]
    process_and_save_partition(bucket_name, file_paths)
