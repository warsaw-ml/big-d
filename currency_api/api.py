from google.cloud import pubsub_v1
import json
import requests 
import time
from google.oauth2 import service_account

class BTC_API:
    def __init__(self):
        self.key = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        self.PROJECT_ID = "bda-wut"#"big-d-project-404815"
        self.TOPIC_NAME = "bitcoin-topic"
        self.topic_path = f"projects/{self.PROJECT_ID}/topics/{self.TOPIC_NAME}"
    def get_data(self):
        # requesting data from url 
        data = requests.get(self.key) 
        data = data.json() 
        return data

    def send_to_pubsub(self, data):
        # Konfiguracja klienta Pub/Sub
        publisher = pubsub_v1.PublisherClient(
            #login using json file
            credentials=service_account.Credentials.from_service_account_file(
                filename='../bda-wut-9a9d2770188c.json'),)#
        #         ../big-d-project-404815-e51da3ccf3cc.json'),
        # )
        # Konwersja danych do formatu JSON
        data_json = json.dumps(data)
        # Publikowanie wiadomości
        try:
            publisher.publish(self.topic_path, data=data_json.encode('utf-8'))
            print(f"Message published : {data_json}")
        except Exception as e:
            print(f"Eror during publishing message: {e}")

    def listen_for_messages(self, wait_time=15):
        while True:
            time.sleep(wait_time)
            data = self.get_data()
            self.send_to_pubsub(data)
            del data
            
if __name__ == "__main__":
    btc_api = BTC_API()
    btc_api.listen_for_messages()