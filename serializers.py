import io
import struct
from writers import SchemalessAvroRecordWriter

_MAGIC_BYTE = 0

class ContextBytesIO(io.BytesIO):

    """
    Wrapper to allow use of BytesIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False

class AvroMessageSerializer(object):

    def __init__(self, schema_json, schema_id):
        self.schema_id = schema_id
        self.writer = SchemalessAvroRecordWriter(schema_json)

    def kafka_avro_encode(self, record):
        with ContextBytesIO() as buf:
            # write the header
            # magic byte
            buf.write(struct.pack('b', _MAGIC_BYTE))
            # write the schema ID in network byte order (big end)
            buf.write(struct.pack('>I', self.schema_id))
            self.writer.write(buf, record)
            return buf.getvalue()