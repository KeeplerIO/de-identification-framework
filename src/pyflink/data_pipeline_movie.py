from data_pipeline import job_data_pipeline

SCHEMA_NAME = 'movie'
KAFKA_RAW_TOPIC='movie-raw-data'
KAFKA_ANONYMISED_TOPIC='movie-anonymised-data'
KEY_NAME="key.key"

job_data_pipeline(
    SCHEMA_NAME,
    KAFKA_RAW_TOPIC,
    KAFKA_ANONYMISED_TOPIC,
    KEY_NAME
)