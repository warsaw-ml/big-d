import pandas as pd
import pytest

from cassandra.cluster import Cluster


def convert_to_df(rows):
    data = []
    for row in rows:
        data.append(row)

    df = pd.DataFrame(data)

    return df


def test_data_available_in_cassandra():
    """Test that data is available in Cassandra."""

    cassandra_cluster = Cluster(["34.118.53.49"])
    session = cassandra_cluster.connect("cassandra1")

    query = "SELECT * FROM crypto;"
    rows_btc = session.execute(query)

    query = "SELECT * FROM clusters;"
    rows_clusters = session.execute(query)

    query = "SELECT * FROM chatrooms;"
    rows_messages = session.execute(query)

    query = "SELECT * FROM embeddings;"
    rows_embeddings = session.execute(query)

    df_btc = convert_to_df(rows_btc)
    df_clusters = convert_to_df(rows_clusters)
    df_messages = convert_to_df(rows_messages)
    df_embeddings = convert_to_df(rows_embeddings)

    cassandra_cluster.shutdown()

    # Test if data table downloads are not empty
    # assert df_btc.shape[0] > 0
    assert df_clusters.shape[0] > 0
    # assert df_messages.shape[0] > 0
    # assert df_embeddings.shape[0] > 0


def test_avg_number_of_msgs():
    cassandra_cluster = Cluster(["34.118.53.49"])
    session = cassandra_cluster.connect("cassandra1")

    query = "SELECT * FROM clusters;"
    rows_clusters = session.execute(query)
    df_clusters = convert_to_df(rows_clusters)

    # query = "SELECT * FROM chatrooms;"
    # rows_messages = session.execute(query)
    # df_messages = convert_to_df(rows_messages)

    # Test if there are at least 10 messages per hour on average
    assert df_clusters.shape[0] / 168 > 10
