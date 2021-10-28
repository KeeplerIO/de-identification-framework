import json
import requests
import os

ATLAS_URI = "http://"+os.environ['ATLAS_HOST']+"/api/atlas/v2"
ATLAS_USER = os.environ['ATLAS_USER']
ATLAS_PASSWORD = os.environ['ATLAS_PASSWORD']

def create_entity(entity):
    entity["guid"] = -1
    payload = { "entity": entity }
    r = requests.post(ATLAS_URI+'/entity', json=payload, auth=(ATLAS_USER, ATLAS_PASSWORD))
    if r.status_code == 200:
        return r.json()["guidAssignments"]["-1"]
    else:
        print(r.text)
        raise Exception('Something went wrong when creating a new entity in Atlas')

def create_pii_type(pii_name):
    print("Creating "+pii_name)
    payload={
      "classificationDefs": [
        {
          "name": pii_name,
          "description":"",
          "attributeDefs": [],
          "superTypes": []
        }
      ],
      "entityDefs": [],
      "enumDefs": [],
      "structDefs": []
    }

    if pii_name != "PII" and pii_name != "NON_PII":
        payload["classificationDefs"][0]["superTypes"].append("PII")

    r = requests.post(ATLAS_URI+'/types/typedefs?type=classification', json=payload, auth=(ATLAS_USER, ATLAS_PASSWORD))
    if r.status_code == 200:
        return r.json()['classificationDefs'][0]['guid']
    else:
        print(r.status_code, r.text)
        raise Exception('Something went wrong when creating a new PII')

def create_pii_if_not_exists(pii_array):
    pii_info = {}
    for pii in pii_array:
        r = requests.get(ATLAS_URI+'/types/classificationdef/name/'+pii, auth=(ATLAS_USER, ATLAS_PASSWORD))
        if r.status_code == 200:
            pii_info[pii] = r.json()['guid']
        elif r.status_code == 404:
            pii_info[pii] = create_pii_type(pii)
        else:
            raise Exception('Something went wrong when retrieving information from Atlas')
    return pii_info
    
def create_field_type(field_type):
    field_type_payload = {
        "typeName": "avro_type",
        "attributes": {
            "name": field_type,
            "qualifiedName": "avro_type@"+field_type
        }
    }
    return create_entity(field_type_payload)
    
def get_field_type_guid(field_type):
    query_params = {
        "limit": 1,
        "offset": 0,
        "query": "where name="+field_type,
        "typeName": "avro_type"
    }
    r = requests.get(ATLAS_URI+'/search/dsl', params=query_params, auth=(ATLAS_USER, ATLAS_PASSWORD))
    
    if r.status_code == 200:
        if "entities" in r.json():
            return r.json()["entities"][0]["guid"]
        else:
            return create_field_type(field_type)
    elif r.status_code == 400:
            return create_field_type(field_type)
    else:
        raise Exception('Something went wrong when retrieving field type information from Atlas')
        
        
def classify_field(field_guid, pii_types):
    create_pii_if_not_exists(["PII", "NON_PII"])
    create_pii_if_not_exists(pii_types)
    for pii in pii_types:
        payload={
            "classification": {
                "typeName": pii,
                "propagate": True,
                "removePropagationsOnEntityDelete": False
            },
            "entityGuids": [field_guid]
        }
        r = requests.post(ATLAS_URI+'/entity/bulk/classification', json=payload, auth=(ATLAS_USER, ATLAS_PASSWORD))
        if r.status_code == 204 or (r.status_code == 400 and r.json()["errorCode"] == "ATLAS-400-00-01A"):
            return
        else:
            raise Exception('Something went wrong when retrieving field type information from Atlas')
        

def create_field(field):

    field_payload = {
        "typeName": "avro_field",
        "attributes": {
            "name": field["name"],
            "qualifiedName": "avro_field@"+field["name"]
        },
        "relationshipAttributes": {
            "type": [
                {
                    "guid": get_field_type_guid(field["type"]),
                    "typeName": "avro_type"
                }
            ]
        }
    }
    
    field_guid = create_entity(field_payload)
    
    if field['metadata']['sensistive_data'] is True:
        pii_types = []
        for pii_type in field['metadata']['data_privacy_assetsment']:
            pii_types.append(pii_type['entity_type'])
        classify_field(field_guid, pii_types)
    else:
        classify_field(field_guid, ["NON_PII"])
    return field_guid

def create_schema(schema, kafka_topic):
    
    schema_payload = {
        "typeName": "avro_schema",
        "attributes": {
            "name": schema["name"],
            "namespace": schema["namespace"],
            "qualifiedName": "avro_schema@"+schema["name"],
            "type": schema["type"]
        },
        "relationshipAttributes": {
            "fields": [],
            "kafka_topics_references": [{"guid":kafka_topic, "typeName": "kafka_topic"}],
            "meanings": []
        }
    }
    
    for field in schema['fields']:
        schema_payload["relationshipAttributes"]["fields"].append({"guid": create_field(field),"typeName": "avro_field"})
            
    create_entity(schema_payload)

def create_kafka_datasource(topic_name):
    kafka_payload = {
        "typeName": "kafka_topic",
        "attributes": {
            "name": topic_name,
            "qualifiedName": "kafk_topic@"+topic_name,
            "topic": topic_name,
            "uri": "kafka@"+topic_name
        }
    }
    return create_entity(kafka_payload)
            