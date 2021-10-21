from kafka import KafkaProducer
from kafka import errors 
from time import sleep

import json
import os

KAFKA_SERVER = os.environ['KAFKA_SERVER']

def write_data(producer, topic, datafile):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file = open(dir_path+datafile, 'r')
    lines = file.readlines()
    for line in lines:
        l = json.loads(line.strip())
        print(l)
        producer.send(topic, value=l)
    file.close()
    producer.flush()
    producer.close()

def create_producer():
    print("Connecting to Kafka brokers")
    for i in range(0, 10):
        try:
            producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER],
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))
            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(20)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

def generate(kafka_topic, data_file):
    producer = create_producer()
    write_data(producer, kafka_topic, data_file)