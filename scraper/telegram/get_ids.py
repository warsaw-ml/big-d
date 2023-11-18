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

with open("data/tg_calls.yaml") as f:
    call_channels = yaml.load(f, Loader=yaml.FullLoader)

with open("data/tg_chatrooms.yaml") as f:
    group_chats = yaml.load(f, Loader=yaml.FullLoader)


api_id = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
client = TelegramClient("anon", api_id, api_hash).start()


def get_forum_topic_ids():
    ids = []

    for forum_name, forum_id in forums.items():
        print(forum_name, forum_id)

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
            # print(topic)
            # print(topic.from_id)
            # print(topic.from_id)
            try:
                ids.append(topic.id)
            except:
                pass

    return []
    # return ids


def get_call_channels_ids():
    ids = []
    for call_name, call_id in call_channels.items():
        ids.append(call_id)

    return ids


def get_group_chat_ids():
    ids = []
    for gc_name, gc_id in group_chats.items():
        ids.append(gc_id)

    return ids


ids_forums = get_forum_topic_ids()
ids_call_channels = get_call_channels_ids()
ids_group_chats = get_group_chat_ids()


all_ids = ids_forums + ids_call_channels + ids_group_chats
print("Number of rooms:", len(all_ids))


# save ids to file
with open("data/tg_ids.txt", "w") as f:
    for id in all_ids:
        f.write(str(id) + "\n")
