# Kafka Producer
import requests
import json
import logging
import time
from kafka import KafkaProducer

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    return producer

def get_weather(producer):
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 51.5,
            "longitude": -0.11,
            "current":"temperature_2m",
        },
    )
    data = response.json()
    producer.send(
        'weather_data_demo',
        value=data
    )
    print(f"Sent data: {response}")
  
def run_producer():
    producer = create_producer()
    logging.info("Producer started. Press Ctrl+C to stop")
    try:
        while True:
            get_weather(producer)
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Producer stopped by user.")
    finally:
        producer.close()
        logging.info("Producer closed.")

if __name__ == "__main__":
    run_producer()
