from inference_model import infer_schema

KAFKA_SERVER='kafka:9092'
KAFKA_TOPIC='mobile-raw-data'
SCHEMA_NAME='mobile'

infer_schema(KAFKA_SERVER,KAFKA_TOPIC,SCHEMA_NAME)