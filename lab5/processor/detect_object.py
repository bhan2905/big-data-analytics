import cv2 as cv
import numpy as np
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from ultralytics import YOLO

consumer = KafkaConsumer(
    'camera_stream',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    max_partition_fetch_bytes=10485760
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


print("Loading YOLOv8...")
model = YOLO('yolov8n.pt')
PERSON_CLASS_ID = 0

print("Processing Server ready...")

for message in consumer:
    try:
        frame_bytes = np.frombuffer(message.value, dtype=np.uint8)
        frame = cv.imdecode(frame_bytes, cv.IMREAD_COLOR)

        if frame is None:
            print("Frame is none, skipping...")
            continue

        results = model(frame, classes=[PERSON_CLASS_ID], verbose=False)

        bounding_boxes = []
        for box in results[0].boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0].tolist())
            confidence = round(float(box.conf[0]), 3)
            bounding_boxes.append({
                "x1": x1, "y1": y1,
                "x2": x2, "y2": y2,
                "confidence": confidence
            })

        person_count = len(bounding_boxes)

        annotated = results[0].plot()
        label = f"People: {person_count}"
        cv.putText(annotated, label, (10, 35),
                   cv.FONT_HERSHEY_SIMPLEX, 1.1, (0, 255, 0), 2)
        cv.imshow('Processing Server - Detection', annotated)

        result_payload = {
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "person_count": person_count,
            "bounding_boxes": bounding_boxes,
            "frame_width": frame.shape[1],
            "frame_height": frame.shape[0]
        }
        producer.send('detection_results', result_payload)

        print(f"[{result_payload['timestamp']}] Detected {person_count} people | "
              f"{len(bounding_boxes)} boxes")

        if cv.waitKey(1) & 0xFF == ord('q'):
            break

    except Exception as e:
        print(f"Error processing frame: {e}")
        continue

cv.destroyAllWindows()
producer.flush(timeout=5)
producer.close()
consumer.close()
print("Processing Server stopped.")