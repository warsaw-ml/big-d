import asyncio
import json
import os
import random
from datetime import datetime, timedelta

import autorootcwd
import pytz
import yaml
from google.cloud import storage
from google.oauth2 import service_account
from rich import print
from telethon import events, functions
from telethon.sync import TelegramClient
from tqdm import tqdm

# GC project variables
PROJECT_ID = "big-d-project-404815"
bucket_name = "big-d-project-master-dataset"

# Load credentials
credentials = service_account.Credentials.from_service_account_file("data/big-d-project-404815-44996acd710d.json")

# Initialize Google Cloud Storage client
client = storage.Client(credentials=credentials, project=PROJECT_ID)

# Get the bucket object
bucket = client.get_bucket(bucket_name)

# Setup Telegram client
api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
client = TelegramClient("anon", api_id, api_hash).start()

# Load IDs of tg rooms to scrape
with open("data/tg_calls.yaml") as f:
    call_channels = yaml.load(f, Loader=yaml.FullLoader)
    ids = list(call_channels.values())


async def get_batch(ids):
    """Scrape a batch of messages from last hour and upload to GCS bucket."""

    message_list = []

    # get timestamp from last hour
    # last_hour_timestamp = datetime.now() - timedelta(hours=10)
    # last_hour_timestamp = last_hour_timestamp.replace(tzinfo=pytz.utc)
    # print(last_hour_timestamp)

    # get messages from all channels
    print("Downloading messages...")
    for id in tqdm(ids):
        messages = await client.get_messages(id, limit=10)
        message_list += messages

    print("Processing messages...")
    message_batch = []
    for msg in tqdm(message_list):
        # get message text and timestamp
        text = msg.text
        timestamp = str(msg.date)

        # filter messages older than from last hour
        # if msg.date < last_hour_timestamp:
        #     continue

        # filter empty messages and useless "gm" messages
        if not text or ("gm" in text.lower() and len(text) < 15):
            continue

        # create unique message id
        message_id = str(msg.peer_id.channel_id) + str(msg.id)

        # idk why this happens but it does
        if not msg.sender:
            # print("no sender!")
            continue

        # get info about sender
        username = msg.sender.username
        user_id = msg.sender.id
        is_bot = False

        # get info about source channel
        channel_id = msg.peer_id.channel_id
        channel_entitiy = await client.get_entity(msg.peer_id)
        channel_name = channel_entitiy.title

        # create final message dataframe
        dataframe = {
            "message_id": message_id,
            "text": text,
            "user_id": user_id,
            "username": username,
            "first_name": None,
            "last_name": None,
            "is_bot": is_bot,
            "channel_id": channel_id,
            "channel_name": channel_name,
            "timestamp": timestamp,
        }
        message_batch.append(dataframe)

    # Sample 100 messages randomly
    if len(message_batch) > 100:
        message_batch = random.sample(message_batch, 100)

    print("Batch len:", len(message_batch))

    # Convert list of dicts to JSON
    data = json.dumps(message_batch)

    # save to file (for testing only)
    # with open("data/batch.json", "w") as f:
    # f.write(data)

    # Get current date and hour
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_hour = datetime.now().strftime("%H")

    # File path in the bucket
    file_path = f"telegram-batch/{current_date}/{current_hour}/batch.json"

    # Create a blob object
    blob = bucket.blob(file_path)

    # Upload the JSON data
    blob.upload_from_string(data=data, content_type="application/json")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_batch(ids))
