from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

# Connect to Cassandra
cassandra_cluster = Cluster(['34.118.38.6'])

# Nazwa tabeli
table_name = 'chatrooms'

# Prepare the INSERT statement
message_id = "123"
text = "test"
username = "John_test"
first_name = "John"
last_name = "Doe"
user_id = "123"
is_bot = False
channel_name = "test_channel"
channel_id = "123"
timestamp = "2023-11-12T13:45:23.456Z"

insert_statement = SimpleStatement(
    f"INSERT INTO {table_name} (message_id, text, username, first_name, last_name, user_id, is_bot, channel_name, channel_id, timestamp) VALUES ('{message_id}', '{text}', '{username}', '{first_name}', '{last_name}', '{user_id}', {is_bot}, '{channel_name}', '{channel_id}', '{timestamp}')",
    consistency_level=ConsistencyLevel.ONE
)
session = cassandra_cluster.connect('bigd')
session.execute(insert_statement)

# Close the Cassandra session and cluster connection
session.shutdown()



# # Nazwa tabeli
table_name = 'embeddings'
message_id = "123"
embedding = [1.0, 2.0, 3.0]

# Prepare the INSERT statement
insert_statement = SimpleStatement(
    f"INSERT INTO {table_name} (message_id, embedding) VALUES ('{message_id}', {embedding})",
    consistency_level=ConsistencyLevel.ONE
)
session = cassandra_cluster.connect('bigd')

session.execute(insert_statement)

# Close the Cassandra session and cluster connection
session.shutdown()