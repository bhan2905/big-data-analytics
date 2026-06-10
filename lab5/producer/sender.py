import cv2 as cv
from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    max_request_size=10485760,
    buffer_memory=33554432
)

capture = cv.VideoCapture(0)

if not capture.isOpened():
    print("Cannot open camera!")
    exit()

print("Sending stream to Kafka... Press 'q' to stop.")

while True:
    ret, frame = capture.read()
    if not ret:
        print("Cannot read frame from camera.")
        break
    
    frame_small = cv.resize(frame, (320, 240))
    _, buffer = cv.imencode('.jpg', frame_small, [cv.IMWRITE_JPEG_QUALITY, 80])
    
    producer.send('camera_stream', buffer.tobytes())

    cv.imshow('Camera (sender)', frame_small)

    if cv.waitKey(1) & 0xFF == ord('q'):
        break

    time.sleep(0.033)

capture.release()
cv.destroyAllWindows()
producer.flush(timeout=10)
producer.close()
print("Sender stopped.")