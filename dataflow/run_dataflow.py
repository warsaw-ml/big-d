import subprocess
import os

WINDOW_SIZE = 1
RUNNER = 'DataflowRunner'
PROJECT_ID = 'big-d-project-404815'
REGION = 'europe-central2'
BUCKET_NAME = 'big-d-project-master-dataset'

command = [
    'python', 
    './full_dataflow.py',
    f'--project={PROJECT_ID}',
    f'--region={REGION}',
    f'--window_size={WINDOW_SIZE}',
    f'--runner={RUNNER}',
    f'--temp_location=gs://{BUCKET_NAME}/temp'
]

try:
    subprocess.run(command, check=True)
except subprocess.CalledProcessError as e:
    print(f"Error: {e}")