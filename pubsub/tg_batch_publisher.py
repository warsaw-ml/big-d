import asyncio
import json
import os

import autorootcwd
import yaml
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from rich import print
from telethon import events
from telethon.sync import TelegramClient

PROJECT_ID = "big-d-project-404815"
TOPIC_NAME = "telegram-batch"
topic_path = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"
credentials = service_account.Credentials.from_service_account_file("data/big-d-project-404815-44996acd710d.json")
publisher = pubsub_v1.PublisherClient(credentials=credentials)

with open("data/tg_ids.txt") as f:
    ids = f.read().splitlines()


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


async def get_batch(ids):
    message_list = []

    for id in ids:
        messages = await client.get_messages(int(id), limit=5)
        message_list += messages

    message_batch = []
    for msg in message_list:
        message_id = str(msg.peer_id.channel_id) + str(msg.id)
        text = msg.text
        username = msg.sender.username
        first_name = msg.sender.first_name
        last_name = msg.sender.last_name
        user_id = msg.sender.id
        is_bot = msg.sender.bot
        channel_id = msg.peer_id.channel_id
        channel_entitiy = await client.get_entity(msg.peer_id)
        channel_name = channel_entitiy.title
        timestamp = str(msg.date)

        dataframe = {
            "message_id": message_id,
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
        print(dataframe)

        message_batch.append(dataframe)

        # publish_message(message_batch)

    # limit number of messages to 100
    if len(message_batch) > 100:
        message_batch = message_batch[:100]

    # publish_message(message_batch)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_batch(ids))
