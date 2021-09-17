from inference_model import infer_schema

KAFKA_SERVER='kafka:9092'
KAFKA_TOPIC='website-raw-data'
SCHEMA_NAME='website'

infer_schema(KAFKA_SERVER,KAFKA_TOPIC,SCHEMA_NAME)