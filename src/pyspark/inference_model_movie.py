from inference_model import infer_schema

KAFKA_SERVER='kafka:9092'
KAFKA_TOPIC='movie-raw-data'
SCHEMA_NAME='movie'

infer_schema(KAFKA_SERVER,KAFKA_TOPIC,SCHEMA_NAME)