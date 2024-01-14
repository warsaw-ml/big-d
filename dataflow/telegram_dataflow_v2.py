import argparse
import json
import logging

import apache_beam as beam
import numpy as np
from apache_beam import DoFn, Pipeline
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from vertexai.language_models import TextEmbeddingModel

from cassandra.cluster import Cluster

PROJECT_ID = "bda-wut"
TOPIC_NAME = "telegram-topic"
TOPIC = f"projects/{PROJECT_ID}/topics/{TOPIC_NAME}"
CASSANDRA_HOST = "34.118.38.6"

CASSANDRA_KEYSPACE = "bigd"
CASSANDRA_TABLE = "chatrooms_stream"

BUCKET_NAME = "bda-wut-project-master-dataset"
PREFIX = "json/centroid_clustering.json"
EMBEDDING_MODEL = "textembedding-gecko@001"


#  python dataflow/telegram_dataflow_v2.py \
#   --runner DataflowRunner \
#   --project bda-wut \
#   --region us-central1 \
#   --temp_location gs://bda-wut-project-cloud-utils/dataflow


class GenerateEmbedding(DoFn):
    def start_bundle(self):
        self.model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)

    def process(self, element):
        data = json.loads(element)
        text_to_embed = data.get("text", "")
        embedding = self.get_embedding(text_to_embed)
        data["embedding"] = embedding
        yield data

    def get_embedding(self, text):
        embeddings = self.model.get_embeddings([text])
        for embedding in embeddings:
            vector = embedding.values
        return vector


class AttachClosestCentroid(DoFn):
    def __init__(self, centroids):
        # centroids is a dictionary of cluster_number: position
        self.centroids = centroids

    def process(self, element):
        embedding = np.array(element["embedding"])

        # Initialize variables to store the minimum distance and corresponding cluster
        min_distance = float("inf")
        closest_cluster = None

        # Iterate through each cluster to find the closest one
        for cluster_number, position in self.centroids.items():
            position = np.array(position)
            embedding = np.array(embedding)
            distance = np.linalg.norm(position - embedding)

            # Update if this is the closest cluster so far
            if distance < min_distance:
                min_distance = distance
                closest_cluster = cluster_number

        # Attach the closest cluster label to the element
        element["cluster"] = str(closest_cluster)
        yield element


class WriteToCassandra(DoFn):
    def process(self, element):
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)

        message_id = element["message_id"]
        text = element["text"]
        username = element["username"]
        first_name = element["first_name"]
        last_name = element["last_name"]
        user_id = element["user_id"]
        is_bot = element["is_bot"]
        channel_name = element["channel_name"]
        channel_id = element["channel_id"]
        timestamp = element["timestamp"]
        embedding = str(element["embedding"])
        cluster_number = element["cluster"]

        session.execute(
            f"INSERT INTO {CASSANDRA_TABLE} (message_id, text, username, first_name, last_name, user_id, is_bot, channel_name, channel_id, timestamp, embedding, cluster) VALUES ('{message_id}', '{text}', '{username}', '{first_name}', '{last_name}', '{user_id}', {is_bot}, '{channel_name}', '{channel_id}', '{timestamp}', '{embedding}', '{cluster_number}')"
        )
        cluster.shutdown()


def load_centroids_from_gcs(bucket_name, prefix):
    bucket_name = "bda-wut-project-master-dataset"
    prefix = "json/centroid_clustering.json"

    # Ścieżka do pliku JSON z kluczem dostępu usługi
    credentials_path = "data/bda-wut-82fcf814c117.json"

    # Utwórz klienta do Google Cloud Storage z użyciem klucza dostępu
    storage_client = storage.Client.from_service_account_json(credentials_path)

    # Inicjalizacja pustego słownika na dane z plików
    centroids = {}

    # Pobierz listę plików w danym prefixie
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    # Przejdź przez listę plików
    for blob in blobs:
        # Upewnij się, że blob jest plikiem JSON
        if (blob.name.endswith(".json")) & ("part" in blob.name):
            # Odczytaj zawartość pliku JSON z Cloud Storage
            raw = blob.download_as_text()
            if len(raw.split("\n")) > 2:
                for line in raw.split("\n")[:-1]:
                    data = json.loads(line)
                    # Dodaj dane do słownika, używając nazwy pliku jako klucza
                    centroids[data["cluster"]] = data["centroid"]
            else:
                data = json.loads(raw)
                # Dodaj dane do słownika, używając nazwy pliku jako klucza
                centroids[data["cluster"]] = data["centroid"]

    return centroids


def run(input_topic, pipeline_args=None):
    pipeline_options = PipelineOptions(
        pipeline_args,
        streaming=True,
        save_main_session=True,
        requirements_file="requirements.txt",
    )

    centroids = load_centroids_from_gcs(BUCKET_NAME, PREFIX)

    with Pipeline(options=pipeline_options) as pipeline:
        _ = (
            pipeline
            | "Read from PubSub" >> ReadFromPubSub(topic=input_topic)
            | "Decode PubSub message" >> beam.Map(lambda x: x.decode("utf-8"))
            | "Generate Embedding" >> beam.ParDo(GenerateEmbedding())
            | "Attach Closest Centroid" >> beam.ParDo(AttachClosestCentroid(centroids))
            | "Write to Cassandra" >> beam.ParDo(WriteToCassandra())
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
        default=TOPIC,
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        pipeline_args,
    )
