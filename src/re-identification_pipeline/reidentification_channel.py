from reidentification import job_data_pipeline

SCHEMA_NAME = 'channel'
KAFKA_RAW_TOPIC='channel-raw-data'
KAFKA_ANONYMISED_TOPIC='channel-anonymised-data'
KEY_NAME="key.key"

job_data_pipeline(
    SCHEMA_NAME,
    KAFKA_RAW_TOPIC,
    KAFKA_ANONYMISED_TOPIC,
    KEY_NAME
)