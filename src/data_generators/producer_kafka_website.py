from producer_kafka import generate

KAFKA_TOPIC='website-raw-data'
DATA_FILE='/../dummy_data/website_data.txt'

generate(KAFKA_TOPIC, DATA_FILE)