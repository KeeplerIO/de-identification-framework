FROM bitnami/spark:3.1.2-debian-10-r90
USER root

WORKDIR /opt/application

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN pip3 install --upgrade pip --user

RUN pip3 install pyspark

RUN pip3 install numpy
RUN pip3 install pandas
RUN pip3 install presidio-analyzer
RUN pip3 install presidio-anonymizer
RUN python -m spacy download en_core_web_lg