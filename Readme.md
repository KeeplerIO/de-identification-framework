# Demo

## 1. Kafka
###  Create Kafka Topic
```
docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic <TOPIC>
```
###  List Topics
```
docker-compose exec kafka kafka-topics --list --zookeeper zookeeper:2181
```
###  Describe Topic
```
docker-compose exec kafka kafka-topics --describe --zookeeper zookeeper:2181 --topic <TOPIC>
```
###  Remove Topic
```
docker-compose exec kafka kafka-topics --zookeeper zookeeper:2181 --topic <TOPIC> --delete
```
###  Purge Topic
```
docker-compose exec kafka kafka-configs --zookeeper zookeeper:2181 --entity-type topics --alter --entity-name <TOPIC> --add-config retention.ms=1000
```
### See messages
```
docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic <TOPIC> --from-beginning
```

## 2. Producer
### Generate movie raw data
```
docker-compose exec producer python /opt/app/data_generators/producer_kafka_movie.py
```
### Generate channel raw data
```
docker-compose exec producer python /opt/app/data_generators/producer_kafka_channel.py
```
### Generate website raw data
```
docker-compose exec producer python /opt/app/data_generators/producer_kafka_website.py
```
### Generate mobile raw data
```
docker-compose exec producer python /opt/app/data_generators/producer_kafka_mobile.py
```

## 3. Spark

### Infer Scheme Channel
```
docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/app/pyspark/inference_model_channel.py
```

### Infer Scheme Movie
```
docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/app/pyspark/inference_model_movie.py
```

### Infer Scheme Website
```
docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/app/pyspark/inference_model_website.py
```

### Infer Scheme Mobile
```
docker-compose exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /opt/app/pyspark/inference_model_mobile.py
```

### Schema Compatibility problems during testing
```
curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"compatibility": "NONE"}' http://localhost:8081/config
```

## 4. Flink (Now it does not run in the Flink cluster.)

### Data pipeline job Channel
```
docker-compose exec producer python /opt/app/pyflink/data_pipeline_channel.py
```
### Data pipeline job Movie
```
docker-compose exec producer python /opt/app/pyflink/data_pipeline_movie.py
```
### Data pipeline job Website
```
docker-compose exec producer python /opt/app/pyflink/data_pipeline_website.py
```

### Data pipeline job Mobile
```
docker-compose exec producer python /opt/app/pyflink/data_pipeline_mobile.py
```