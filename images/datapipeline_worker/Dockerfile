FROM python:3.7

RUN apt-get update
RUN apt install librdkafka-dev -y

RUN set -ex; \
  python -m pip install --upgrade pip; \
  pip install kafka-python; \
  pip install faker; \
  pip install numpy; \
  pip install pandas; \
  pip install confluent-kafka; \ 
  pip install requests; \
  pip install fastavro; \
  pip install uuid;

RUN pip3 install avro
RUN pip3 install cryptography

RUN pip3 install presidio-analyzer
RUN pip3 install presidio-anonymizer
RUN python -m spacy download en_core_web_lg

#CMD ["python", "-u", "./generate_source_data.py"]