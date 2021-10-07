docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic movie-raw-data
docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic movie-anonymised-data
docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic channel-raw-data
docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic channel-anonymised-data
docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic mobile-raw-data
docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic mobile-anonymised-data
docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic website-raw-data
docker-compose exec kafka kafka-topics --create --if-not-exists --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic website-anonymised-data