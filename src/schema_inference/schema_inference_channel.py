from schema_inference import infer_schema

KAFKA_SERVER='kafka:29092'
KAFKA_TOPIC='channel-raw-data'
SCHEMA_NAME='channel'

infer_schema(KAFKA_SERVER,KAFKA_TOPIC,SCHEMA_NAME)