import json
import os
import random
import re

import pandas as pd
import plotly.express as px
import streamlit as st
import umap.umap_ as umap

from cassandra.cluster import Cluster

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "data/big-d-project-404815-44996acd710d.json"


def get_data_mock():
    path = "data/telegram"

    data = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    json_data = json.load(f)
                    data.append(json_data)

    df = pd.concat([pd.DataFrame(d) for d in data], ignore_index=True)

    # replace Nan with string "None"
    df = df.fillna("None")

    return df


def get_data_cassandra():
    cassandra_cluster = Cluster(["34.118.103.223"])
    session = cassandra_cluster.connect("cassandra1")

    # Execute a query to retrieve data from the table
    query = "SELECT * FROM clusters;"
    rows = session.execute(query)

    # Convert the query result to a pandas DataFrame
    data = []
    for row in rows:
        data.append(row)

    df = pd.DataFrame(data)

    # Close the Cassandra session
    cassandra_cluster.shutdown()

    return df


data = get_data_cassandra()
exit()


# Pull data from Cassandra
df = get_data_mock()
embeddings = df.embedding.tolist()
texts = df.text.tolist()
users = df.username.tolist()
channel_names = df.channel_name.tolist()
umap_embeddings = umap.UMAP(n_neighbors=15, min_dist=0.1, n_components=2).fit_transform(embeddings)
df_embeddings_umap = pd.DataFrame(umap_embeddings, columns=["x", "y"])

# generate list of random cluster numbers for each datapoints
cluster_number = [random.randint(1, 10) for _ in range(len(df_embeddings_umap))]

# Set layout
st.set_page_config(layout="wide")

# Title of the web app
st.title("Dashboard")

# Set subheader
st.subheader("Telegram messages latent space")

# Create an interactive scatter plot using Plotly
fig = px.scatter(df_embeddings_umap, x="x", y="y", color=cluster_number, color_continuous_scale="Viridis")

# Update the hovertemplate
fig.update_traces(
    hovertemplate='Message: "%{hovertext}"</b><br><br>Username: <i>%{customdata[0]}</i></b><br>Channel: <i>%{customdata[1]}</i>',
    hovertext=texts,
    customdata=list(zip(users, channel_names)),
    marker=dict(size=3),
)

# Set size of the plot
fig.update_layout(
    autosize=False,
    width=1200,
    height=700,
    margin=dict(l=0, r=0, b=0, t=0, pad=0),
)

# Set x-axis and y-axis limits
fig.update_xaxes(range=[-10, 25])
fig.update_yaxes(range=[-10, 15])

# Plot
st.plotly_chart(fig)


def get_tickers(df):
    # Step 1: Extract Tickers and Channel IDs
    # Extract tickers while making them case-insensitive and without numbers
    df["tickers"] = df["text"].str.findall(r"\$[A-Za-z]+", flags=re.IGNORECASE)

    # Step 2: Normalize and Pair Tickers with Channel IDs
    # Normalize tickers to upper case and pair them with channel_id
    pairs = df.apply(lambda x: [(ticker.upper(), x["channel_id"]) for ticker in x["tickers"]], axis=1)

    # Flatten the list of pairs
    flat_pairs = [item for sublist in pairs for item in sublist]

    # Convert to DataFrame
    pairs_df = pd.DataFrame(flat_pairs, columns=["ticker", "channel_id"])

    # Step 3: Count Unique Channel IDs for Each Ticker
    # Group by ticker and count unique channel_ids
    grouped = pairs_df.groupby("ticker")["channel_id"].nunique()

    # Count the total mentions of each ticker
    total_mentions = pairs_df["ticker"].value_counts()

    # Step 4: Create a Summary DataFrame
    # Create a new DataFrame
    ticker_summary_df = pd.DataFrame(
        {
            "ticker": grouped.index,
            "total_mentions": total_mentions[grouped.index].values,
            "unique_channel_count": grouped.values,
        }
    )

    # Sort the ticker_summary_df by total_mentions
    sorted_ticker_summary = ticker_summary_df.sort_values(by="total_mentions", ascending=False)
    result = sorted_ticker_summary.head(20).reset_index(drop=True)

    return result


# Display the top tickers
st.subheader("Top 20 Tickers Mentioned")
ticker_df = get_tickers(df)
st.dataframe(ticker_df, height=738)

# Optional: Display the raw data as a table
st.subheader("Example Messages")
df_display = df[["text", "username", "channel_name", "timestamp"]].sample(1000)
st.dataframe(df_display)
