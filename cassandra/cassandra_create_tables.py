from cassandra.cluster import Cluster

# Replace these values with your Cassandra cluster information
cluster = Cluster(['34.118.103.223'])
session = cluster.connect('cassandra1')

# Define CQL queries to create tables
chatrooms_cql = """
CREATE TABLE IF NOT EXISTS chatrooms (
    message_id TEXT PRIMARY KEY,
    text TEXT,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    user_id TEXT,
    is_bot BOOLEAN,
    channel_name TEXT,
    channel_id TEXT,
    timestamp TIMESTAMP
);
"""

embeddings_cql = """
CREATE TABLE IF NOT EXISTS embeddings (
    message_id TEXT PRIMARY KEY,
    embedding LIST<FLOAT>
);
"""

crypto_cql = """
CREATE TABLE IF NOT EXISTS crypto (
    symbol TEXT PRIMARY KEY,
    price TEXT,
    timestamp TIMESTAMP
);
"""

cluster_cql = """
CREATE TABLE IF NOT EXISTS clusters (
    message_id TEXT PRIMARY KEY,
    cluster INT
);
"""

# Execute CQL queries to create tables
session.execute(chatrooms_cql)
session.execute(embeddings_cql)
session.execute(crypto_cql)
session.execute(cluster_cql)

# Close the Cassandra session and cluster connection
session.shutdown()
cluster.shutdown()
