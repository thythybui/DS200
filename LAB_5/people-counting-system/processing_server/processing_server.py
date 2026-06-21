"""
SERVER 2 - PROCESSING SERVER
Nhiệm vụ: tiêu thụ (consume) khung hình từ Kafka topic `camera-frames`,
chạy mô hình object detection (YOLOv8, lớp "person") để xác định các
bounding box của người trong ảnh, rồi publish kết quả lên Kafka topic
`detection-results` cho Storage Server.

Vì là Kafka consumer group, có thể chạy nhiều instance song song
(scale-out) để tăng throughput xử lý khi lượng khung hình tăng cao -
đây là điểm thể hiện rõ tư duy xử lý dữ liệu lớn (horizontal scaling)
thay vì chỉ chạy 1 tiến trình xử lý tuần tự.
"""

import os
import json
import time
import base64
import random
import logging

import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PROCESSING] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "camera-frames")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "detection-results")
GROUP_ID = os.getenv("CONSUMER_GROUP", "processing-server-group")
MODEL_NAME = os.getenv("MODEL_NAME", "yolov8n.pt")
CONF_THRESHOLD = float(os.getenv("CONF_THRESHOLD", "0.4"))
USE_MOCK_DETECTION = os.getenv("USE_MOCK_DETECTION", "false").lower() == "true"

PERSON_CLASS_ID = 0  # id lớp "person" trong bộ nhãn COCO mà YOLO dùng


def decode_frame(image_b64: str) -> np.ndarray:
    data = base64.b64decode(image_b64)
    arr = np.frombuffer(data, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        max_poll_records=10,
    )


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
        retries=5,
    )


class Detector:
    """Bọc model YOLO; hỗ trợ chế độ mock để test pipeline không cần tải weight."""

    def __init__(self):
        self.mock = USE_MOCK_DETECTION
        if not self.mock:
            from ultralytics import YOLO  # import trễ để mock mode không cần cài torch
            logger.info("Dang tai mo hinh YOLO: %s", MODEL_NAME)
            self.model = YOLO(MODEL_NAME)
        else:
            logger.warning("USE_MOCK_DETECTION=true -> dung bounding box gia lap, KHONG chay YOLO that")

    def detect(self, frame: np.ndarray):
        if self.mock:
            return self._mock_detect(frame)
        return self._yolo_detect(frame)

    def _yolo_detect(self, frame: np.ndarray):
        results = self.model.predict(frame, conf=CONF_THRESHOLD, classes=[PERSON_CLASS_ID], verbose=False)
        boxes = []
        for r in results:
            for box in r.boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                boxes.append({
                    "x1": round(x1, 1), "y1": round(y1, 1),
                    "x2": round(x2, 1), "y2": round(y2, 1),
                    "confidence": round(float(box.conf[0]), 3),
                })
        return boxes

    @staticmethod
    def _mock_detect(frame: np.ndarray):
        h, w = frame.shape[:2]
        count = random.randint(0, 6)
        boxes = []
        for _ in range(count):
            bw, bh = random.randint(40, 100), random.randint(80, 180)
            x1 = random.randint(0, max(1, w - bw))
            y1 = random.randint(0, max(1, h - bh))
            boxes.append({
                "x1": float(x1), "y1": float(y1),
                "x2": float(x1 + bw), "y2": float(y1 + bh),
                "confidence": round(random.uniform(0.4, 0.95), 3),
            })
        return boxes


def main():
    detector = Detector()
    consumer = build_consumer()
    producer = build_producer()

    logger.info("Lang nghe topic '%s' ...", INPUT_TOPIC)

    processed = 0
    for message in consumer:
        start = time.time()
        payload = message.value

        try:
            frame = decode_frame(payload["image_b64"])
        except Exception as exc:
            logger.warning("Bo qua frame loi decode: %s", exc)
            continue

        boxes = detector.detect(frame)
        inference_ms = round((time.time() - start) * 1000, 1)

        result = {
            "frame_id": payload["frame_id"],
            "camera_id": payload["camera_id"],
            "seq": payload.get("seq"),
            "captured_at": payload["timestamp"],
            "processed_at": time.time(),
            "inference_ms": inference_ms,
            "width": payload["width"],
            "height": payload["height"],
            "people_count": len(boxes),
            "boxes": boxes,
        }

        producer.send(OUTPUT_TOPIC, key=payload["camera_id"], value=result)

        processed += 1
        if processed % 20 == 0:
            logger.info(
                "Da xu ly %d frame | so nguoi frame gan nhat=%d | %.1fms",
                processed, len(boxes), inference_ms,
            )


if __name__ == "__main__":
    main()
