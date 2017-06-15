from serializers import AvroMessageSerializer
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka import Producer as KafkaProducer
import json
from datetime import datetime
import sys
import base64

# TODO: factor out all configs to separate module
topic_name='bidder_log_t12'
error_topic_name='bidder_log_t12_error'
schema_subject='bidder_log_t12-value'
compression='lz4'

schema_reg_url='http://graphite-101:8081'

SRClient = CachedSchemaRegistryClient(url=schema_reg_url)
schema_tuple = SRClient.get_latest_schema(subject=schema_subject)

SCHEMA = {'schema_id': schema_tuple[0], 'avro_schema': schema_tuple[1], 'schema_json': schema_tuple[1].to_json()}

err_out = None

def write_fatal_error(rec, type='AVRO'):
   global err_out
   base64_data = base64.b64encode(rec)
   line = json.dumps(dict({'type': type, 'data': base64_data}))
   if not err_out:
       # TODO: generate unique error file name, log type + ts probably makes sense
      err_out = open('res/out.err', 'w')
   err_out.write(line)
   err_out.write('\n')

def log_error(err):
   tsStr = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
   sys.stderr.write('%s ERROR %s\n' % (tsStr, err))

def error_callback(err, msg=None):
   log_error(err)

def on_delivery_callback(err, msg_metadata):
   if err:
      log_error(err)
   write_fatal_error(msg_metadata.value())

s = AvroMessageSerializer(schema_json=SCHEMA['schema_json'], schema_id=SCHEMA['schema_id'])

p = KafkaProducer(**{'client.id': 'kafka_t3.py',
                     'bootstrap.servers': 'kafka-01:9092',
                     'log.connection.close': False,
                     'api.version.request': True,
                     'message.send.max.retries': 10,
                     'batch.num.messages': 1000,
                     'delivery.report.only.error': True,
                     'default.topic.config': {
                         'request.required.acks': 1,
                         'message.timeout.ms': 10000
                     },
                     'error_cb': error_callback,
                     'on_delivery': on_delivery_callback })

with open('test_data/captains_log_8_2017.06.12.21') as inf:
   for line in inf:
      avro_record = None
      record = json.loads(line)
      try:
         avro_record = s.kafka_avro_encode(json.loads(line))
      except Exception as E:
         error_record = json.dumps(dict({'error': str(E), 'record': record}))
         p.produce(topic=error_topic_name, value=error_record)
      else:
         p.produce(topic=topic_name, value=avro_record)
   p.flush()

if err_out:
   err_out.close()