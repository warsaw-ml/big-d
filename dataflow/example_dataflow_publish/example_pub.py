from google.cloud import pubsub_v1
import json
import time

PROJECT_ID = 'bda-wut'
TOPIC_NAME = 'bitcoin-topic'
TOPIC = f'projects/{PROJECT_ID}/topics/{TOPIC_NAME}'

def publish_messages(project, topic, messages):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)

    for message in messages:
        data = json.dumps(message).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"Published message: {data}")
        future.result()

if __name__ == "__main__":
    sample_data = [
        {"symbol": "BTC", "price": 50000.0},
        {"symbol": "ETH", "price": 3000.0},
        # Add more sample data as needed
    ]

    publish_messages(PROJECT_ID, TOPIC_NAME, sample_data)
