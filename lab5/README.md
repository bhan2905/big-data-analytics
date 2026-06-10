# Hệ thống đếm người thời gian thực với Big Data

Hệ thống phát hiện và đếm số người trong camera sử dụng kiến trúc Big Data với Apache Kafka, YOLOv8 và Apache Cassandra.

---

## Kiến trúc hệ thống

| Server | File | Chức năng |
|---|---|---|
| Camera Server | `producer/sender.py` | Đọc frame từ camera, gửi lên Kafka |
| Processing Server | `processor/detect_object.py` | Nhận frame, chạy YOLOv8, trả về bounding boxes |
| Storage Server | `storage/sinker.py` | Nhận kết quả, lưu vào Cassandra |

---

## Công nghệ sử dụng

- **Apache Kafka** - Truyền frame và kết quả giữa các server
- **Apache Cassandra** - Lưu trữ kết quả theo thời gian thực
- **YOLOv8 (Ultralytics)** - Mô hình nhận diện đối tượng
- **OpenCV** - Đọc và xử lý frame từ camera
- **Docker** - Chạy Kafka, Zookeeper, Cassandra

---

## Cấu trúc thư mục

```
LAB5/
├── producer/
│   └── sender.py # Camera Server
├── processor/
│   └── detect_object.py # Processing Server
├── storage/
│   ├── sinker.py # Storage Server
│   └── export_results.py # Xuất CSV kết quả
├── output/
│   └── results.csv # Kết quả
└── docker-compose.yml # Kafka + Zookeeper + Cassandra
```

---

## Yêu cầu cài đặt

### 1. Docker
Tải và cài Docker Desktop

### 2. Python packages
```bash
pip install kafka-python opencv-python ultralytics torch torchvision
pip install cassandra-driver eventlet futurist
```

---

## Hướng dẫn chạy

### Bước 1 — Khởi động Docker
```bash
docker-compose up -d
docker ps
```

### Bước 2 — Chạy Storage Server
```bash
python sinker.py
```

### Bước 3 — Chạy Processing Server
```bash
python detect_object.py
```

### Bước 4 — Chạy Camera Server
```bash
python sender.py
```
Cửa sổ camera hiện lên. Hệ thống bắt đầu hoạt động.

### Bước 5 — Xuất kết quả
```bash
python export_results.py
```
File CSV xuất ra tại `results.csv`.

---

## Kết quả

### Mẫu kết quả CSV
```
timestamp,person_count,bounding_boxes,frame_width,frame_height
2026-06-10 14:56:49,1,"[{""x1"": 106, ""y1"": 109, ""x2"": 270, ""y2"": 239, ""confidence"": 0.821}]",320,240
2026-06-10 14:56:42,1,"[{""x1"": 108, ""y1"": 109, ""x2"": 269, ""y2"": 239, ""confidence"": 0.835}]",320,240
```
