import json
from datetime import datetime

from apache_beam.io import gcsio
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from vertexai.language_models import TextEmbeddingModel
import apache_beam as beam
from apache_beam import DoFn, ParDo


PROJECT_ID = 'big-d-project-404815'
TOPIC = 'test-topic'
EMBEDDING_MODEL = 'textembedding-gecko@001'
MESSAGE_FIELDS = ['message_id', 'text', 'username', 'first_name', 'last_name', 'user_id', 'is_bot', 'channel_name', 'channel_id', 'timestamp']

with open('examples.json', 'r') as f:
    INPUT_DATA = json.load(f)


class WriteToGCS(DoFn):
    def __init__(self):
        self.output_path = 'gs://big-d-project-master-dataset/test'

    def process(self, element):
        
        # Create a random filename, but different from the previous ones

        ts_format = "%Y-%m-%d %H:%M:%S:%f"
        timestamp = datetime.now().strftime(ts_format)
        filename = f"{self.output_path}/{timestamp}.json"

        # Extract message bodies from the batch
        messages = element

        with beam.io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            # Write JSON containing the list of messages
            f.write(json.dumps(messages).encode())



class ProcessPubsubMessage(DoFn):
    
    def start_bundle(self):
        self.model = TextEmbeddingModel.from_pretrained(EMBEDDING_MODEL)

    def process(self, element):
        try:
            data = element
            text_to_embed = data.get('text', '')
            embedding = self.get_embedding(text_to_embed)
            data['embedding'] = embedding
            # yield json dump of data
            yield data

        except Exception as e:
            print(f"Error processing message: {e}")

    def get_embedding(self, text):
        embeddings = self.model.get_embeddings([text])
        for embedding in embeddings:
            vector = embedding.values
        return vector



class TestMyPipeline(unittest.TestCase):

    def test_process_pubsub_message(self):
       
        expected_processed_data = [{**item, 'embedding': [0] * 768} for item in INPUT_DATA]
        expected_embedding_length = 768

        # Run the pipeline in test mode
        with TestPipeline() as p:
            # Mock the input PCollection
            input_pcoll = p | beam.Create(INPUT_DATA)

            # Call your pipeline function with the mocked input
            processed_pcoll = (
                input_pcoll
                | "Process message" >> beam.ParDo(ProcessPubsubMessage())
            )

            assert_that(
                processed_pcoll,
                lambda elements: all(len(item['embedding']) == expected_embedding_length for item in elements)
            )


    def test_filter_out_invalid_message(self):
       

        input = INPUT_DATA + [
            {"text": "message 1", "embedding": [0.1] * 1024},
            {"text": "message 2", "embedding": [0.2] * 1024},
            {"text": "invalid message"},
            {"text": "invalid message"},

        
        ]

        expected_data = INPUT_DATA

        with TestPipeline() as p:
            
            input_pcoll = p | beam.Create(input)

            filtered_pcoll = (
                input_pcoll
                | "Filter out invalid message" >> beam.Filter(lambda x: all(field in x for field in MESSAGE_FIELDS) and x is not None)
            )

            assert_that(filtered_pcoll, equal_to(expected_data))


    def test_write_to_gcs(self):
  
        # Run the pipeline in test mode
        with TestPipeline() as p:
            # Mock the input PCollection
            input_pcoll = p | beam.Create(INPUT_DATA)

            # Call your pipeline function with the mocked input
            gcs_output = (
                input_pcoll
                | "Write to GCS" >> ParDo(WriteToGCS())
            )

            # assert that there are 5 files written to GCS
            assert_that(gcs_output, lambda elements: len(elements) == 5)
        

        gcs_files = gcsio.GcsIO().list_prefix('gs://big-d-project-master-dataset/test')
        json_files = [file for file in gcs_files if file.endswith('.json')]
        self.assertEqual(len(json_files), len(INPUT_DATA))
        # clear test files
        for json_file in json_files:
            gcsio.GcsIO().delete(json_file)



if __name__ == '__main__':
    unittest.main()
