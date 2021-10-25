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

import os

import requests

from cryptography.fernet import Fernet

ATLAS_URI = "http://"+os.environ['ATLAS_HOST']+"/api/atlas/v2"
ATLAS_USER = os.environ['ATLAS_USER']
ATLAS_PASSWORD = os.environ['ATLAS_PASSWORD']
KAFKA_SERVER = os.environ['KAFKA_SERVER']
SCHEMA_SERVER = os.environ['SCHEMA_SERVER']

DELTA_TIME = -2

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

def atlas_get_guid_by_query(query):
    r = requests.get(ATLAS_URI+'/search/dsl', params=query, auth=(ATLAS_USER, ATLAS_PASSWORD))
    if r.status_code == 200:
        if "entities" in r.json():
            return r.json()["entities"][0]["guid"]
        else:
            print(r.status_code)
            print(r.text)
            raise Exception('Schema does not exists in Atlas')
    else:
        raise Exception('Something went wrong when exectuting Atlas query')
        
def atlas_get_by_guid(guid):
    r = requests.get(ATLAS_URI+'/entity/guid/'+guid, auth=(ATLAS_USER, ATLAS_PASSWORD))
    if r.status_code == 200:
        return r.json()
    else:
        raise Exception('Something went wrong when retrieving entity information from Atlas')

def retrieve_schema(schema_name):
    query_params = {
        "limit": 1,
        "offset": 0,
        "query": "where name="+schema_name,
        "typeName": "avro_schema"
    }
    
    schema_guid = atlas_get_guid_by_query(query_params)
    schema_info = atlas_get_by_guid(schema_guid)

    schema = {}
    schema['name'] = schema_info['entity']['attributes']['name']
    schema['namespace'] = schema_info['entity']['attributes']['namespace']
    schema['type'] = schema_info['entity']['attributes']['type']
    schema['fields'] = []
    
    for field in schema_info['entity']['attributes']['fields']:
        field_info = atlas_get_by_guid(field['guid'])
        field_type = atlas_get_by_guid(field_info['entity']['attributes']['type'][0]['guid'])
        f = {
           "name": field_info['entity']['attributes']['name'],
           "type": field_type['entity']['attributes']['name'],
           "metadata": {}
        }
        #Check if the field is a PII
        if (len(field_info['entity']['classifications']) > 0) and (any(pii['typeName'] != 'NON_PII' for pii in field_info['entity']['classifications'])):
            f['metadata']['sensistive_data'] = True
            f['metadata']['pii_types'] = []
            for pii in field_info['entity']['classifications']:
                f['metadata']['pii_types'].append(pii['typeName'])
        else:
            f['metadata']['sensistive_data'] = False
            f['metadata']['pii_types'] = []
        
        schema['fields'].append(f)
        
    return schema

def get_pii_entities_types(schema):
    entities_per_field = {}

    for field in schema["fields"]:
        if field['metadata']['sensistive_data'] == True:
            entities_per_field[field['name']] = field['metadata']['pii_types']
        else:
            entities_per_field[field['name']] = []

    return entities_per_field

def serializer_function(message, ctx):
    return message

# The key storage should be in an external system
def retrieve_key(key_name):
    return open("/opt/app/keys/"+key_name, "rb").read()

def encrypt(str, key):
    return Fernet(key).encrypt(str.encode()).decode()

def validate_date(date_text):
    try:
        if date_text != datetime.strptime(date_text, "%Y-%m-%d").strftime('%Y-%m-%d'):
            raise ValueError
        return True
    except ValueError:
        return False

# TODO Have a variable shifting per user or record id and store it.
def shifting(date):
    if validate_date(date):
        datetime_obj= datetime.strptime(date, '%d-%M-%Y')
        datetime_shifted=datetime_obj + timedelta(days=DELTA_TIME)
        return datetime_shifted.strftime('%d-%M-%Y')
    else:
        return date
        
def deidentifcation_pipeline(schema_name,topic_input,topic_output,key_name ):

    print("Retrieving Key ....")
    encryption_key = retrieve_key(key_name)

    print(encryption_key)

    print("Retrieving Schema ....")
    schema = retrieve_schema(schema_name)
    print(schema)

    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_SERVER})

    print("Creating Avro Serializer ...")
    avro_serializer = AvroSerializer(schema_str=json.dumps(schema),
                                         schema_registry_client=schema_registry_client,
                                         to_dict=serializer_function)

    consumer = create_consumer([KAFKA_SERVER], topic_input)
    producer = create_avro_producer([KAFKA_SERVER], avro_serializer)

    print("Extracting metadata PII information ...")
    pii_per_field = get_pii_entities_types(schema)
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
        producer.produce(topic_output, key=str(uuid4()), value=message.value)
