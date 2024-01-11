import subprocess
import os

WINDOW_SIZE = 1
RUNNER = 'DataflowRunner'
PROJECT_ID = 'bda-wut'
REGION = 'europe-central2'
BUCKET_NAME = 'bda-wut-project-master-dataset'
REPOSITORY = 'bda-wut-repo'
DOCKER_NAME = 'dataflow/df-docker:latest'

command = [
    'python', 
    './bitcoin_dataflow.py',
    f'--project={PROJECT_ID}',
    f'--region={REGION}',
    f'--window_size={WINDOW_SIZE}',
    f'--runner={RUNNER}',
    f'--sdk_container_image={REGION}-docker.pkg.dev/{PROJECT_ID}/{REPOSITORY}/{DOCKER_NAME}',
    f'--sdk_location=container',
    f'--temp_location=gs://{BUCKET_NAME}/temp',
    f'--staging_location=gs://{BUCKET_NAME}/staging',

]

try:
    subprocess.run(command, check=True)
except subprocess.CalledProcessError as e:
    print(f"Error: {e}")