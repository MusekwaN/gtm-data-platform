from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import os

try:
    admin = KafkaAdminClient(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        request_timeout_ms=10000
    )
    topics = sorted(admin.list_topics())
    print('KAFKA_TOPICS=' + ','.join(topics))
except NoBrokersAvailable as e:
    print('ERROR=NoBrokersAvailable:' + str(e))
except Exception as e:
    print('ERROR=' + type(e).__name__ + ':' + str(e))
