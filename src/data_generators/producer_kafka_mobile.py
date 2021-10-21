from producer_kafka import generate

KAFKA_TOPIC='mobile-raw-data'
DATA_FILE='/../dummy_data/mobile_data.txt'

generate(KAFKA_TOPIC, DATA_FILE)