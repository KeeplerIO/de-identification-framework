from time import sleep
from datetime import datetime, timedelta

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

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

DELTA_TIME = +2

def create_avro_consumer(kafka_servers, avro_deserializer, topic):
    for i in range(0, 10):
        try:

            string_deserializer = StringDeserializer('utf_8')

            consumer_conf = {'bootstrap.servers': ",".join(kafka_servers),
                            'key.deserializer': string_deserializer,
                            'value.deserializer': avro_deserializer,
                            'auto.offset.reset': "earliest",
                            'group.id': str(uuid4()),
                            }
                            
            print(consumer_conf)

            consumer = DeserializingConsumer(consumer_conf)
            consumer.subscribe([topic])
            print("Connected to Kafka")
            return consumer
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

def deserializer_function(message, ctx):
    return message

# The key storage should be in an external system
def retrieve_key(key_name):
    return open("/opt/app/keys/"+key_name, "rb").read()

def decrypt(str, key):
    return Fernet(key).decrypt(str.encode()).decode()

def validate_date(date_text):
    try:
        if date_text != datetime.strptime(date_text, '%d-%M-%Y').strftime('%d-%M-%Y'):
            raise ValueError
        return True
    except ValueError:
        return False

# TODO Have a variable shifting per user and store it.
def shifting(date):
    if validate_date(date):
        datetime_obj= datetime.strptime(date, '%d-%M-%Y')
        datetime_shifted=datetime_obj + timedelta(days=DELTA_TIME)
        return datetime_shifted.strftime('%d-%M-%Y')
    else:
        return date
        
        
def deanonymizer(text, entities, operators):

    for entity in entities:
        operator = None
        if entity['entity_type'] in operators.keys():
            operator = operators[entity['entity_type']]

        if operator is not  None:
            return operator(text[entity['start']:entity['end']])
            
    #No re-identification method available
    return text
    
        
def job_reidentification_pipeline(schema_name,topic_input,key_name ):

    print("Retrieving Key ....")
    encryption_key = retrieve_key(key_name)

    print("Retrieving Schema ....")
    schema = retrieve_schema(schema_name)

    sr_conf = {'url': SCHEMA_SERVER}
    schema_registry_client = SchemaRegistryClient(sr_conf)

    print("Creating Avro Deserializer ...")
    avro_deserializer = AvroDeserializer(schema_str=json.dumps(schema), schema_registry_client=schema_registry_client)
                                         
    print("Connecting Kafka")
    consumer = create_avro_consumer([KAFKA_SERVER], avro_deserializer, topic_input)

    print("Extracting metadata PII information ...")
    pii_per_field = get_pii_entities_types(schema)

    while True:
        message=consumer.poll(timeout=2.0)
        if message is not None:
            msg_value = message.value()
            for key in msg_value.keys():
                pii_entities = pii_per_field[key]
                if len(pii_entities):
                    analyzer_results=None
                    if len(pii_entities) > 1: # Free text field
                        ## Outside the purpose of the demo.
                        ### The analysis of the free text fields during the de-identification process should be saved for later reversal.
                        ### Another way would be to use predefined masks according to the type of PII and then parse the free text field.
                        #### For example John Smith -> PERSON-dbcbcf7f7af8f851538eef7b8e58c5bee0b8cfdac4a
                        #### Where PERSON is the type of PII, followed by the encrypted value
                        analyzer_results = []
                    else:
                        analyzer_results=[
                            { "start":0, "end":len(str(msg_value[key])), "entity_type":pii_entities[0] }
                        ]
                        
                    #As Presidio only allows to make one of its deanonymizer function with decrypt, we change this part to a custom function.
                    deanonymized_result = deanonymizer(
                        text=str(msg_value[key]),
                        entities=analyzer_results,
                        operators={
                            # The De-Identification methods based on hashing are reversible only using brute force
                            "USER_ID": lambda x: decrypt(x, encryption_key),
                            "DATE_TIME" : lambda x : shifting(x),
                            #"CREDIT_CARD" : OperatorConfig("hash", {"hash_type": "sha256" }), # only brute force
                            #"CUSTOM_CREDIT_CARD" : OperatorConfig("hash", {"hash_type": "sha256" }), # only brute force
                            "DOMAIN_NAME" : lambda x: x,
                            # "PHONE_NUMBER" : OperatorConfig("hash", {"hash_type": "sha256" }), # only brute force
                        }
                    )
                    
                    msg_value[key] = deanonymized_result
    
            # The data de-identification method must be well chosen, 
            # taking into account possible future re-identification, 
            # since for example methods based on hashes or single-value substitutions make backtracking impossible.
            print(msg_value)