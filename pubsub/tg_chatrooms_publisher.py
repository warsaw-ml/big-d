import json
import os

import autorootcwd
import yaml
from google.cloud import pubsub_v1
from rich import print
from telethon import events
from telethon.sync import TelegramClient

PROJECT_ID = "big-d-project-404815"
TOPIC_NAME = "telegram-topic"
topic_path = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"
publisher = pubsub_v1.PublisherClient()

with open("data/tg_chatrooms.yaml") as f:
    tg_chatrooms = yaml.load(f, Loader=yaml.FullLoader)
    ids = list(tg_chatrooms.values())

api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
client = TelegramClient("anon", api_id, api_hash).start()


def publish_message(data):
    data = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print(f"Published message: {data}")
    # print error if any
    future.result()


@client.on(events.NewMessage(chats=ids, incoming=True))
async def handler(event):
    text = event.text
    username = event.sender.username
    first_name = event.sender.first_name
    last_name = event.sender.last_name
    user_id = event.sender.id
    is_bot = event.sender.bot
    timestamp = str(event.date)
    # timestamp = str(event.message.date) # I think this might be older timestamp if the message is forwarded from another channel

    source_entitiy = await client.get_entity(event.peer_id)
    channel_name = source_entitiy.title
    channel_id = source_entitiy.id

    # convert to json
    data = {
        "text": text,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "user_id": user_id,
        "is_bot": is_bot,
        "channel_name": channel_name,
        "channel_id": channel_id,
        "timestamp": timestamp,
    }

    print(data)

    # Publish data to Pub/Sub
    publish_message(data)


print("Listenning for messages from ids:", ids)
client.run_until_disconnected()
