from schema_inference import infer_schema

KAFKA_SERVER='kafka:29092'
KAFKA_TOPIC='mobile-raw-data'
SCHEMA_NAME='mobile'

infer_schema(KAFKA_SERVER,KAFKA_TOPIC,SCHEMA_NAME)