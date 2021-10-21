from producer_kafka import generate

KAFKA_TOPIC='channel-raw-data'
DATA_FILE='/../dummy_data/channel_data.txt'

generate(KAFKA_TOPIC, DATA_FILE)