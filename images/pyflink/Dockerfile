FROM apache/flink:1.13.1-scala_2.12-java8
ARG FLINK_VERSION=1.13.1

# Install pyflink
#RUN set -ex; \
RUN  apt-get update;
RUN  apt-get -y install python3;
RUN  apt-get -y install python3-pip;
RUN  apt-get -y install python3-dev;
RUN  ln -s /usr/bin/python3 /usr/bin/python;
RUN  ln -s /usr/bin/pip3 /usr/bin/pip;
  
RUN apt-get update
RUN python -m pip install --upgrade pip
RUN pip install kafka-python
RUN pip install apache-flink


# Download connector libraries
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/${FLINK_VERSION}/flink-sql-connector-kafka_2.12-${FLINK_VERSION}.jar; \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.12/${FLINK_VERSION}/flink-sql-connector-elasticsearch7_2.12-${FLINK_VERSION}.jar;


RUN echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml;

WORKDIR /opt/flink
