from data_pipeline import job_data_pipeline

KAFKA_SERVER = ['kafka:9092']
SCHEMA_SERVER = "http://kafka-schema-registry:8081"
SCHEMA_NAME = 'mobile.avsc'
KAFKA_RAW_TOPIC='mobile-raw-data'
KAFKA_ANONYMISED_TOPIC='mobile-anonymised-data'
KEY_NAME="key.key"

job_data_pipeline(
    SCHEMA_SERVER,
    SCHEMA_NAME,
    KAFKA_SERVER,
    KAFKA_RAW_TOPIC,
    KAFKA_ANONYMISED_TOPIC,
    KEY_NAME
)