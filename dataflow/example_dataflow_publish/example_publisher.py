from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json

# Replace with your values
PROJECT_ID = 'big-d-project-404815'
TOPIC_NAME = 'telegram-topic'

publisher = pubsub_v1.PublisherClient()

topic_path = f'projects/{PROJECT_ID}/topics/{TOPIC_NAME}'

def publish_message(data):
    data = json.dumps(data).encode('utf-8')
    future = publisher.publish(topic_path, data=data)
    print(f"Published message: {data}")
    # print error if any
    future.result()

# data from json file
with open('example_data.json') as f:
    data = json.load(f)

# Publish data to Pub/Sub
publish_message(data)
