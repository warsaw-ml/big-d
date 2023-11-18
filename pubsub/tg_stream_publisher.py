import json
import os
from datetime import datetime

import autorootcwd
import yaml
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from rich import print
from telethon import events, functions
from telethon.sync import TelegramClient

PROJECT_ID = "big-d-project-404815"
TOPIC_NAME = "telegram-topic"
topic_path = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"
credentials = service_account.Credentials.from_service_account_file("data/big-d-project-404815-44996acd710d.json")
publisher = pubsub_v1.PublisherClient(credentials=credentials)

with open("data/tg_ids.txt") as f:
    ids = f.read().splitlines()

ids1 = [int(i) for i in ids if isinstance(i, str) and i.isnumeric()]
ids2 = [i for i in ids if isinstance(i, str) and not i.isnumeric()]
ids = ids1 + ids2

api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
client = TelegramClient("anon", api_id, api_hash).start()


def publish_message(data):
    data = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print(f"Published message: {data}")
    try:
        future.result()  # Wait for publish to complete
    except Exception as e:
        print(f"An error occurred: {e}")


@client.on(events.NewMessage(chats=ids, incoming=True))
async def handler(event):
    text = None
    username = None
    first_name = None
    last_name = None
    user_id = None
    is_bot = None
    timestamp = None
    channel_name = None
    channel_id = None

    text = event.text
    timestamp = str(event.date)

    sender = event.sender
    if sender:
        username = sender.username
        first_name = sender.first_name
        last_name = sender.last_name
        user_id = sender.id
        is_bot = sender.bot

    try:
        source_entitiy = await client.get_entity(event.peer_id)
        channel_name = source_entitiy.title
        channel_id = source_entitiy.id
    except Exception as e:
        pass

    # convert to json
    data = {
        "message_id": str(channel_id) + str(event.message.id),
        "text": text,
        "user_id": user_id,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "is_bot": is_bot,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "timestamp": timestamp,
    }

    print(data)

    # Publish data to Pub/Sub
    publish_message(data)


print("Listenning for messages from ids:", ids)
client.run_until_disconnected()
