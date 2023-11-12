import os
from datetime import datetime

import autorootcwd
import yaml
from rich import print
from telethon import events, functions
from telethon.sync import TelegramClient

# load forums from yaml
with open("data/tg_forums.yaml") as f:
    forums = yaml.load(f, Loader=yaml.FullLoader)


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

    print("Number of rooms:", len(ids))

    return ids


ids = get_topics()

# save ids to file
with open("data/tg_forum_ids.txt", "w") as f:
    for id in ids:
        f.write(str(id) + "\n")
