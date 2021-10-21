from data_pipeline import job_data_pipeline

SCHEMA_NAME = 'website'
KAFKA_RAW_TOPIC='website-raw-data'
KAFKA_ANONYMISED_TOPIC='website-anonymised-data'
KEY_NAME="key.key"

job_data_pipeline(
    SCHEMA_NAME,
    KAFKA_RAW_TOPIC,
    KAFKA_ANONYMISED_TOPIC,
    KEY_NAME
)