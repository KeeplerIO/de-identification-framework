from producer_kafka import generate

KAFKA_TOPIC='movie-raw-data'
DATA_FILE='/../dummy_data/movie_data.txt'

generate(KAFKA_TOPIC, DATA_FILE)