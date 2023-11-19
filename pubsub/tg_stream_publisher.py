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

# Setup GC Pub/Sub client
PROJECT_ID = "big-d-project-404815"
TOPIC_NAME = "telegram-topic"
topic_path = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"
credentials = service_account.Credentials.from_service_account_file("data/big-d-project-404815-44996acd710d.json")
publisher = pubsub_v1.PublisherClient(credentials=credentials)

# Setup Telegram client
api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
client = TelegramClient("anon", api_id, api_hash).start()

# Load IDs of tg rooms to scrape
with open("data/tg_groupchats.yaml") as f:
    call_channels = yaml.load(f, Loader=yaml.FullLoader)
    ids = list(call_channels.values())


def get_forum_ids():
    with open("data/tg_forums.yaml") as f:
        call_channels = yaml.load(f, Loader=yaml.FullLoader)
        forum_ids = list(call_channels.values())

    ids = []
    for forum_id in forum_ids:
        forum_topics = client(
            functions.channels.GetForumTopicsRequest(
                channel=forum_id,
                offset_date=datetime.now(),
                offset_id=0,
                offset_topic=0,
                limit=100,
            )
        )

        for topic in forum_topics.topics:
            print(topic.title)
            ids.append(topic.from_id)

    return ids


# add ids from forums
ids += get_forum_ids()


# Send message to GC Pub/Sub
def publish_message(data):
    data = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print(f"Published message: {data}")
    try:
        future.result()  # Wait for publish to complete
    except Exception as e:
        print(f"An error occurred: {e}")


# Listen for messages from provided ids
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

    # filter empty messages and useless "gm" messages
    if not text or ("gm" in text.lower() and len(text) < 15):
        return

    timestamp = str(event.date)

    sender = event.sender
    if sender:
        username = sender.username

        try:
            first_name = sender.first_name
            last_name = sender.last_name
            is_bot = sender.bot
        except Exception as e:
            pass

        user_id = sender.id

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

    # publish data to Pub/Sub
    publish_message(data)


print("Listenning for messages from ids:", ids)
print("number of group chats:", len(ids))
client.run_until_disconnected()
