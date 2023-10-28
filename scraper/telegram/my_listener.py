import asyncio
import os
from datetime import datetime

import autorootcwd
from rich import print
from telethon import events, functions
from telethon.sync import TelegramClient

forums = {
    "floppingblue": 1849376414,
    "ape_aperton": 183422978,
    "gainz": 1854179726,
}
channels = {
    "Z": 1939729776,
    "solstice": "solsticegambles",
}

api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")


client = TelegramClient("anon", api_id, api_hash).start()


def get_topics():
    forum_topics = client(
        functions.channels.GetForumTopicsRequest(
            channel=forums["floppingblue"],
            offset_date=datetime.now(),
            offset_id=0,
            offset_topic=0,
            limit=100,
        )
    )

    ids = []
    for topic in forum_topics.topics:
        print(topic.title, topic.id)
        ids.append(topic.from_id)
        # ids.append(topic.id)

    # print(ids)
    print("Number of rooms:", len(ids))
    print("Listening for messages...")

    return ids


ids = get_topics()


@client.on(events.NewMessage(chats=ids, incoming=True))
async def handler(event):
    print("NEW MESSAGE!")
    print(event.raw_text)

    # download media like .webm
    # if event.media:
    #     media = await client.download_media(event.media)
    #     print(media)
    #     print(type(media))


client.run_until_disconnected()
