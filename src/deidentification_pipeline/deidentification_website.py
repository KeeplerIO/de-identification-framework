from deidentifcation import deidentifcation_pipeline

SCHEMA_NAME = 'website'
KAFKA_RAW_TOPIC='website-raw-data'
KAFKA_ANONYMISED_TOPIC='website-anonymised-data'
KEY_NAME="key.key"

deidentifcation_pipeline(
    SCHEMA_NAME,
    KAFKA_RAW_TOPIC,
    KAFKA_ANONYMISED_TOPIC,
    KEY_NAME
)