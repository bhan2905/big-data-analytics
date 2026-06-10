import json
from kafka import KafkaConsumer

from datetime import datetime
import eventlet
eventlet.monkey_patch()

from cassandra.cluster import Cluster

print("Connecting to Cassandra...")

cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS people_counter
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('people_counter')

session.execute("""
    CREATE TABLE IF NOT EXISTS detection_results (
        id          UUID PRIMARY KEY,
        timestamp   TIMESTAMP,
        person_count INT,
        bounding_boxes TEXT,
        frame_width INT,
        frame_height INT
    )
""")

print("Cassandra ready...")

consumer = KafkaConsumer(
    'detection_results',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Storage Server ready...")

insert_stmt = session.prepare("""
    INSERT INTO detection_results
        (id, timestamp, person_count, bounding_boxes, frame_width, frame_height)
    VALUES (uuid(), ?, ?, ?, ?, ?)
""")

for message in consumer:
    try:
        data = message.value

        timestamp    = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S')
        person_count = data['person_count']
        boxes_json   = json.dumps(data['bounding_boxes'])
        frame_width  = data['frame_width']
        frame_height = data['frame_height']

        session.execute(insert_stmt, (
            timestamp,
            person_count,
            boxes_json,
            frame_width,
            frame_height
        ))

        print(f"[{data['timestamp']}] Data stored — {person_count} people | "
              f"frame {frame_width}x{frame_height}")

    except Exception as e:
        print(f"Error storing data: {e}")
        continue

consumer.close()
session.shutdown()
cluster.shutdown()
print("Storage Server stopped.")