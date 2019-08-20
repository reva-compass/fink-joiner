# -*- coding: utf-8 -*-
"""
:copyright: (c) 2019 by Urban Compass, Inc.
:author: azfar.aziz
"""

from kafka import KafkaProducer, KafkaConsumer
import time
import base64
import logging
import fastavro
from six import BytesIO
import json


MESSAGE_SCHEMA = \
  {
    "name": "pipeline_message",
    "type": "record",
    "namespace": "pipeline",
    "fields": [
      {"name": "trace_id", "type": "string"},
      {"name": "data_version", "type": "string"},
      {"name": "ts_created_at", "type": "string"},
      {"name": "payload", "type": "string"},
    ]
  }

def to_json(data):
  return '{"trace_id":"%s", "data_version":"%s", "ts_created_at":"%s", "payload":"%s"}' % (data['trace_id'], data['data_version'], data['ts_created_at'], data['payload'])


def read_joined():

  consumer = KafkaConsumer(bootstrap_servers='b-1.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-2.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-3.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-4.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-5.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-6.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092',
                           auto_offset_reset='earliest',
                           consumer_timeout_ms=1000000)

  consumer.subscribe(['data_listings_joined_aspen_mls_rets_av_1'])

  data = []
  while True:
    for message in consumer:
      decoded = BytesIO(base64.b64decode(message.value))
      avro = fastavro.schemaless_reader(decoded, MESSAGE_SCHEMA)
      #json_msg = json.dumps(avro)
      #print("%s\n%s" % (avro['trace_id'], avro['payload']))
      print(avro)
      write_joined(avro)

  consumer.close()

  return data


def write_joined(msg):
  bs = 'b-1.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-2.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-3.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-4.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-5.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092,b-6.listings-pipeline-beta.jlg1k0.c1.kafka.us-east-1.amazonaws.com:9092'
  #b-1.data-search-developmen.8thi2l.c1.kafka.us-east-1.amazonaws.com:9092,b-2.data-search-developmen.8thi2l.c1.kafka.us-east-1.amazonaws.com:9092,b-3.data-search-developmen.8thi2l.c1.kafka.us-east-1.amazonaws.com:9092, b-4.data-search-developmen.8thi2l.c1.kafka.us-east-1.amazonaws.com:9092,b-5.data-search-developmen.8thi2l.c1.kafka.us-east-1.amazonaws.com:9092,b-6.data-search-developmen.8thi2l.c1.kafka.us-east-1.amazonaws.com:9092')
  #producer = KafkaProducer(bootstrap_servers='localhost:9092')
  topic = 'data_listings_json_joined_aspen_mls_rets_av_1'

  producer = KafkaProducer(bootstrap_servers=bs,
                           value_serializer=lambda x: json.dumps(x).encode('utf-8'))

  #stringio = BytesIO()
  #avro_data = fastavro.schemaless_writer(
  #	stringio,
  #	MESSAGE_SCHEMA,
  #	{'trace_id': msg['trace_id'],
  #	 'data_version': msg['data_version'],
  #	 'ts_created_at': msg['ts_created_at'],
  #	 'payload': msg['payload']
  #	}
  #)

  #print("writing", avro_data)
  #producer.send(topic, stringio.getvalue())
  producer.send(topic, msg)

  producer.close()

if __name__ == "__main__":
  data = read_joined()
