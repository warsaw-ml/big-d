import asyncio
import csv
import os
from datetime import datetime

import autorootcwd
import yaml
from rich import print
from telethon import functions
from telethon.sync import TelegramClient

api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")

client = TelegramClient("anon", api_id, api_hash).start()


def get_data_sources():
    with open("data/data_sources.yaml", "r") as file:
        data_sources = yaml.safe_load(file)
    return data_sources


def get_forum_topic_ids():
    data_sources = get_data_sources()

    ids = []

    for source in data_sources["forums"]:
        entity_id = data_sources["forums"][source]

        forum_topics_entities = client(
            functions.channels.GetForumTopicsRequest(
                channel=entity_id,
                offset_date=datetime.now(),
                offset_id=0,
                offset_topic=0,
                limit=100,
            )
        )

        for topic in forum_topics_entities.topics:
            ids.append(topic)
            print(topic.title, topic.id)

    print("Number of rooms:", len(ids))

    return ids


async def get_channel_ids():
    data_sources = get_data_sources()
    print(data_sources)

    entity_id = data_sources["channels"]["SolsticeGambles"]
    channel_entity = await client.get_entity(entity_id)

    print(channel_entity)


def get_topic_messages(topics):
    L_Lab_Topic_Id = None
    for t in topics:
        if t.title == "TG - L's Lab [All]":
            L_Lab_Topic_Id = t.from_id

    max_messages = 10000
    messages = client.get_messages(L_Lab_Topic_Id, limit=max_messages)

    last_msg = None
    with open("data/examples/L_Lab.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Message"])

        # filter messages from bots
        filters = ["@ProficyPriceBot", "@misstrenchbot", "@RickBurpBot", "@AlphaGardenersBot"]

        for msg in messages:
            text = msg.message

            # check if any of filters is present in messsage
            if any(filter in text for filter in filters):
                continue

            # escape newlines
            text = text.replace("\n", "\\n ")

            # sometimes there are duplicates for some reason so lets remove them
            if text == last_msg:
                continue

            # write to csv
            writer.writerow([text])
            last_msg = text

            # print(text)


if __name__ == "__main__":
    ids = get_forum_topic_ids()
    get_topic_messages(ids)
