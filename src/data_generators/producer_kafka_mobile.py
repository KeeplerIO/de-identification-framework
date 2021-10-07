from kafka import KafkaProducer
from kafka import errors 
from time import sleep

import json
import os

KAFKA_SERVERS=['kafka:29092']
KAFKA_TOPIC='mobile-raw-data'
DATA_FILE='/../dummy_data/mobile_data.txt'

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

def create_producer(kafka):
    print("Connecting to Kafka brokers")
    for i in range(0, 10):
        try:
            producer = KafkaProducer(bootstrap_servers=kafka,
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))
            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(20)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

if __name__ == '__main__':
    producer = create_producer(KAFKA_SERVERS)
    write_data(producer, KAFKA_TOPIC, DATA_FILE)