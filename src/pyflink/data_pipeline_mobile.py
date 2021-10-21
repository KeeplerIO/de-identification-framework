from data_pipeline import job_data_pipeline

SCHEMA_NAME = 'mobile'
KAFKA_RAW_TOPIC='mobile-raw-data'
KAFKA_ANONYMISED_TOPIC='mobile-anonymised-data'
KEY_NAME="key.key"

job_data_pipeline(
    SCHEMA_NAME,
    KAFKA_RAW_TOPIC,
    KAFKA_ANONYMISED_TOPIC,
    KEY_NAME
)