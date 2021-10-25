from reidentification import job_reidentification_pipeline

SCHEMA_NAME = 'movie'
KAFKA_ANONYMISED_TOPIC='movie-anonymised-data'
KEY_NAME="key.key"

job_reidentification_pipeline(
    SCHEMA_NAME,
    KAFKA_ANONYMISED_TOPIC,
    KEY_NAME
)