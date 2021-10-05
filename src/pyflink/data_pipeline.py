from kafka import KafkaConsumer, KafkaProducer, errors
from time import sleep
from datetime import datetime, timedelta

from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities.engine import AnonymizerResult, OperatorConfig
from presidio_anonymizer.entities import RecognizerResult

from confluent_kafka import Producer
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

import sys
sys.path.append('/opt/app/custom_recognizers/user_recognizer')
import user_recognizer
sys.path.append('/opt/app/custom_recognizers/custom_card_recognizer')
import custom_card_recognizer

from uuid import uuid4

import json

from cryptography.fernet import Fernet

def create_consumer(servers, topic):
    for i in range(0, 10):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
            print("Consumer connected to Kafka")
            return consumer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(20)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

def create_producer(servers):
    for i in range(0, 10):
        try:
            producer = KafkaProducer(bootstrap_servers=servers,
                            value_serializer=lambda x: json.dumps(x).encode('utf-8'))
            print("Producer connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(20)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

def create_avro_producer(kafka, avro_serializer):
    print("Connecting to Kafka brokers")
    for i in range(0, 10):
        try:
            producer = SerializingProducer(
                {
                    'bootstrap.servers': ",".join(kafka),
                    'key.serializer': StringSerializer('utf_8'),
                    'value.serializer': avro_serializer
                }
            )
            print("Connected to Kafka")
            return producer
        except :
            print("Waiting for brokers to become available")
            sleep(20)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

#TODO check the schema register
def retrieve_schema(schema):
    jsonFormatSchema = open("/opt/app/schemas/"+schema, "r").read()
    return json.loads(jsonFormatSchema)

def get_pii_entities_types(schema, name):
    entities_per_field = {}

    for field in schema["fields"]:
        entities_per_field[field['name']] = []
        for entity in field['metadata']['data_privacy_assetsment']:
            entities_per_field[field['name']].append(entity["entity_type"])

    return entities_per_field

def serializer_function(message, ctx):
    return message

# The key storage should be in an external system
def retrieve_key(key_name):
    return open("/opt/app/keys/"+key_name, "rb").read()

def encrypt(str, key):
    return Fernet(key).encrypt(str.encode()).decode()

# TODO Have a variable shifting per user and store it.
def shifting(date):
    if date != "PII":
        datetime_obj= datetime.strptime(date, '%d-%M-%Y')
        datetime_shifted=datetime_obj + timedelta(days=-2)
        return datetime_shifted.strftime('%d-%M-%Y')
    else:
        return date
        
def job_data_pipeline(schema_server,schema_name,kafka_servers,topic_input,topic_output,key_name ):

    print("Retrieving Key ....")
    encryption_key = retrieve_key(key_name)

    print(encryption_key)

    print("Retrieving Schema ....")
    schema = retrieve_schema(schema_name)
    print(schema)

    schema_registry_client = SchemaRegistryClient({'url': schema_server})

    print("Creating Avro Serializer ...")
    avro_serializer = AvroSerializer(schema_str=json.dumps(schema),
                                         schema_registry_client=schema_registry_client,
                                         to_dict=serializer_function)

    consumer = create_consumer(kafka_servers, topic_input)
    #producer = create_producer(kafka_servers)
    producer = create_avro_producer(kafka_servers, avro_serializer)

    print("Extracting metadata PII information ...")
    pii_per_field = get_pii_entities_types(schema, "from")
    print(pii_per_field)

    print("Creating Presidio AnalyzerEngine ....")
    registry = RecognizerRegistry()
    registry.load_predefined_recognizers()
    registry.add_recognizer(user_recognizer.get_recognizer())
    registry.add_recognizer(custom_card_recognizer.get_recognizer())

    analyzer = AnalyzerEngine(registry=registry)

    print("Creating Presidio AnonymizerEngine ....")
    anonymizer = AnonymizerEngine()

    for message in consumer:
        for key in message.value.keys():
            pii_entities = pii_per_field[key]
            if len(pii_entities):
                analyzer_results=None
                if len(pii_entities) > 1: # Free text field
                    analyzer_results = analyzer.analyze(text=str(message.value[key]), entities=pii_entities, language='en')                               
                else:
                    analyzer_results=[
                        RecognizerResult(entity_type=pii_entities[0], start=0, end=len(str(message.value[key])), score=1 )
                    ]
                anonymized_result = anonymizer.anonymize(
                    text=str(message.value[key]),
                    analyzer_results=analyzer_results,
                    operators={
                        "USER_ID": OperatorConfig("custom", {"lambda":  lambda x: encrypt(x, encryption_key)}),
                        "DATE_TIME" : OperatorConfig("custom",{"lambda": lambda x: shifting(x)}),
                        "CREDIT_CARD" : OperatorConfig("hash", {"hash_type": "sha256" }),
                        "CUSTOM_CREDIT_CARD" : OperatorConfig("hash", {"hash_type": "sha256" }),
                        "DOMAIN_NAME" : OperatorConfig("custom", {"lambda": lambda x: x }),
                        "PHONE_NUMBER" : OperatorConfig("hash", {"hash_type": "sha256" }),
                    }
                ).to_json()
                message.value[key] = json.loads(anonymized_result)['text']

        print(message.value)

        #producer.send(topic_output, message.value)
        producer.produce(topic_output, key=str(uuid4()), value=message.value)
