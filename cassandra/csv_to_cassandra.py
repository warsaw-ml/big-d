from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel

# Connect to Cassandra
cassandra_cluster = Cluster(['34.118.103.223'])
session = cassandra_cluster.connect('cassandra1')

# Specify the CSV file path and table name
csv_file_path = './clusters.csv'
table_name = 'clusters'

# Open and read the CSV file
with open(csv_file_path, 'r') as file:
    # Skip the header line
    file.readline()

    # Iterate through the remaining lines and insert data
    for line in file:
        message_id, cluster = line.strip().split(',')
        cluster = int(cluster)
        # Prepare the INSERT statement
        insert_statement = SimpleStatement(
            f"INSERT INTO {table_name} (message_id, cluster) VALUES (%s, %s)",
            consistency_level=ConsistencyLevel.QUORUM
        )

        # Execute the INSERT statement
        session.execute(insert_statement, (message_id, cluster))

# Close the Cassandra connection
cassandra_cluster.shutdown()
