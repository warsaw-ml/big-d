from cassandra.cluster import Cluster

# Replace these values with your Cassandra cluster information
cluster = Cluster(["34.118.38.6"])
session = cluster.connect("bigd")

# Define CQL queries to create tables
chatrooms_cql = """
CREATE TABLE IF NOT EXISTS chatrooms_stream (
    message_id TEXT PRIMARY KEY,
    text TEXT,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    user_id TEXT,
    is_bot BOOLEAN,
    channel_name TEXT,
    channel_id TEXT,
    timestamp TIMESTAMP,
    embedding TEXT,
    cluster TEXT,
);
"""

# remove table
# chatrooms_cql = """
# DROP TABLE chatrooms_stream
# """

# crypto_cql = """
# CREATE TABLE IF NOT EXISTS crypto (
#     symbol TEXT PRIMARY KEY,
#     price DOUBLE,
#     timestamp TIMESTAMP
# );
# """

# chatrooms_crypto_merged_cql = """
# CREATE TABLE IF NOT EXISTS chatrooms_crypto_merged (
# ...
# );
# """


# Execute CQL queries to create tables
session.execute(chatrooms_cql)
# session.execute(crypto_cql)
# session.execute(chatrooms_crypto_merged_cql)

# Close the Cassandra session and cluster connection
session.shutdown()
cluster.shutdown()
