from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import json

import batch_presidio as presidio

from datetime import datetime

def read_kafka_topic(kafka_broker, topic):

    spark = SparkSession.builder.appName("pyspark-inference-job").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    df_json = (spark.read
               .format("kafka")
               .option("kafka.bootstrap.servers", kafka_broker)
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .option("failOnDataLoss", "false")
               .load()
               # filter out empty values
               .withColumn("value", expr("string(value)"))
               .filter(col("value").isNotNull())
               .select("value"))
    
    # decode the json values
    df_read = spark.read.json(
      df_json.rdd.map(lambda x: x.value), multiLine=True)
  
    # drop corrupt records
    if "_corrupt_record" in df_read.columns:
        df_read = (df_read
                   .filter(col("_corrupt_record").isNotNull())
                   .drop("_corrupt_record"))

    return df_read

def add_metadata(df):
 
  batch_analyzer = presidio.BatchAnalyzerEngine()

  df_dict = df.toPandas().to_dict(orient="list")
  analyzer_results = batch_analyzer.analyze_dict(df_dict, language="en")

  now = datetime.now()

  metadata_df = {
      "sensistive_data": False,
      "inference_method": "presidio",
      "last_inference_date": str(now),
      "data_privacy_assetsment": []
  }

  for r in analyzer_results:
    analysis = []
    metadata_row = metadata_df
    for x in r.recognizer_results[0]:
      aux = x.to_dict()
      del aux['end']
      del aux['start']
      analysis.append(aux)

    metadata_row['data_privacy_assetsment'] = analysis
    if len(analysis) > 0:
      metadata_row['sensistive_data'] = True
    else:
      metadata_row['sensistive_data'] = False

    df = df.withColumn(r.key, col(r.key).alias("", metadata=metadata_row))

  return df

def infer_schema(kafka_server, kafka_topic, schema_name):
  df = read_kafka_topic(kafka_server,kafka_topic)
  df_with_metadata = add_metadata(df)

  schema = json.loads(df_with_metadata.schema.json())
  print(schema)
  schema["type"] = "record"
  schema["name"] = schema_name
  schema["namespace"] = "keepler.pluto."+schema_name
  print('/opt/app/schemas/'+schema_name+'.avsc')
  obj = open('/opt/app/schemas/'+schema_name+'.avsc', 'w')
  obj.write(json.dumps(schema))
  obj.close
