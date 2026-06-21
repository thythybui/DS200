import os
import time
import base64
import json
import uuid
import logging

import cv2
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CAMERA] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FRAMES_TOPIC = os.getenv("FRAMES_TOPIC", "camera-frames")
VIDEO_SOURCE = os.getenv("VIDEO_SOURCE", "0")  # "0" = webcam mặc định, hoặc đường dẫn file / rtsp://...
CAMERA_ID = os.getenv("CAMERA_ID", "cam-01")
TARGET_FPS = float(os.getenv("TARGET_FPS", "5"))
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "80"))


def get_video_capture(source: str) -> cv2.VideoCapture:
    """Hỗ trợ cả webcam (index dạng số) lẫn file video / RTSP url."""
    if source.isdigit():
        return cv2.VideoCapture(int(source))
    return cv2.VideoCapture(source)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=20,
        batch_size=64 * 1024,
        compression_type="gzip",
        retries=5,
    )


def encode_frame(frame) -> str:
    ok, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
    if not ok:
        raise RuntimeError("Không thể encode frame thành JPEG")
    return base64.b64encode(buffer).decode("utf-8")


def main():
    producer = build_producer()
    cap = get_video_capture(VIDEO_SOURCE)

    if not cap.isOpened():
        logger.error("Không mở được nguồn video: %s", VIDEO_SOURCE)
        return

    frame_interval = 1.0 / TARGET_FPS if TARGET_FPS > 0 else 0
    frame_count = 0
    is_file_source = not VIDEO_SOURCE.isdigit()

    logger.info(
        "Bat dau doc camera '%s' (nguon=%s, target_fps=%s, topic=%s)",
        CAMERA_ID, VIDEO_SOURCE, TARGET_FPS, FRAMES_TOPIC,
    )

    try:
        while True:
            start = time.time()
            ret, frame = cap.read()

            if not ret:
                if is_file_source:
                    # File video kết thúc -> phát lại từ đầu để mô phỏng luồng camera liên tục
                    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    continue
                logger.warning("Mất kết nối nguồn camera, thử đọc lại...")
                time.sleep(1)
                continue

            frame_count += 1
            message = {
                "frame_id": str(uuid.uuid4()),
                "camera_id": CAMERA_ID,
                "seq": frame_count,
                "timestamp": time.time(),
                "width": frame.shape[1],
                "height": frame.shape[0],
                "image_b64": encode_frame(frame),
            }

            producer.send(FRAMES_TOPIC, key=CAMERA_ID, value=message)

            if frame_count % 50 == 0:
                logger.info("Da gui %d khung hinh len topic '%s'", frame_count, FRAMES_TOPIC)

            elapsed = time.time() - start
            sleep_time = frame_interval - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
    except KeyboardInterrupt:
        logger.info("Dung camera server theo yeu cau nguoi dung")
    finally:
        cap.release()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
