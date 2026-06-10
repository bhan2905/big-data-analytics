import eventlet
eventlet.monkey_patch()

from cassandra.cluster import Cluster
import csv
import os

os.makedirs('D:/lab5/output', exist_ok=True)

cluster = Cluster(['localhost'], port=9042)
session = cluster.connect('people_counter')

rows = list(session.execute("SELECT * FROM detection_results"))

with open('D:/lab5/output/results.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['timestamp', 'person_count', 'bounding_boxes', 
                     'frame_width', 'frame_height'])
    for row in rows:
        writer.writerow([row.timestamp, row.person_count, 
                        row.bounding_boxes, row.frame_width, row.frame_height])

print(f"Exported {len(rows)} records to D:/lab5/output/results.csv")
cluster.shutdown()