from cassandra.cluster import Cluster

# Replace these values with your Cassandra cluster information
cluster = Cluster(['34.118.38.6'])
session = cluster.connect('bigd')

# Define the SELECT query
select_crypto_query = "SELECT * FROM crypto"

# Execute the SELECT query
result = session.execute(select_crypto_query)

# Print the results
for row in result:
    print(row)

# Close the Cassandra session and cluster connection
session.shutdown()
cluster.shutdown()
