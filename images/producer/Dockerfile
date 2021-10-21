FROM python:3.7

RUN apt-get update
RUN apt install librdkafka-dev -y

RUN set -ex; \
  python -m pip install --upgrade pip; \
  pip install kafka-python;