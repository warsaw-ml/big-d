import asyncio
import json

import autorootcwd
import pytest
import yaml

from pubsub.tg_batch_publisher import get_batch


def load_source_ids(file_path):
    with open(file_path) as f:
        sources = yaml.load(f, Loader=yaml.FullLoader)
        ids = list(sources.values())
    return ids


# @pytest.mark.parametrize("yaml_file", ["data/tg_calls.yaml", "data/tg_groupchats.yaml", "data/tg_forums.yaml"])
def test_all_sources_have_at_least_one_id(yaml_file):
    # Load source ids from yaml file
    ids = load_source_ids(yaml_file)

    # Assert that at least one id was loaded
    assert len(ids) > 0


# @pytest.mark.asyncio
# @pytest.mark.parametrize("yaml_file", ["data/tg_calls.yaml"])
# async def test_get_batch(yaml_file):
#     # Define a list of test ids
#     test_ids = load_source_ids(yaml_file)

#     # Call the function with the test ids
#     result = await get_batch(test_ids)

#     # Check that the result is a string (since get_batch returns a JSON string)
#     assert isinstance(result, str)

#     # Convert the result back to a list of dictionaries
#     result = json.loads(result)

#     # Check that the result is a list
#     assert isinstance(result, list)

#     # Check that each item in the list is a dictionary with the expected keys
#     for item in result:
#         assert isinstance(item, dict)
#         assert set(item.keys()) == set(
#             ["message_id", "text", "user_id", "username", "first_name", "last_name", "is_bot", "channel_id", "channel_name", "timestamp"]
#         )
