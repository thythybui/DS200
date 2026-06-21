import os
import sys
import json
import time
import uuid
import base64
from datetime import datetime, timezone

import cv2
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "processing_server"))

os.environ.setdefault("USE_MOCK_DETECTION", "true")  # không cần tải YOLO weight để chạy smoke test
from processing_server import Detector, decode_frame  # noqa: E402


def make_synthetic_frame(width=640, height=480, n_shapes=3, seed=None) -> np.ndarray:
    """Sinh một khung hình giả lập (không phải ảnh người thật) chỉ để kiểm thử
    luồng encode/decode JPEG <-> base64 giữa các server."""
    rng = np.random.default_rng(seed)
    frame = np.full((height, width, 3), 30, dtype=np.uint8)
    for _ in range(n_shapes):
        x1, y1 = int(rng.integers(0, width - 100)), int(rng.integers(0, height - 150))
        w, h = int(rng.integers(40, 100)), int(rng.integers(80, 150))
        color = tuple(int(c) for c in rng.integers(60, 255, size=3))
        cv2.rectangle(frame, (x1, y1), (x1 + w, y1 + h), color, -1)
    return frame


def encode_frame(frame, quality=80) -> str:
    ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    assert ok, "encode that bai"
    return base64.b64encode(buf).decode("utf-8")


def run(num_frames: int = 10):
    detector = Detector()  # mock mode (USE_MOCK_DETECTION=true)
    results = []

    for i in range(num_frames):
        frame = make_synthetic_frame(seed=i)

        # ---- mô phỏng Camera Server: encode + đóng gói message ----
        camera_msg = {
            "frame_id": str(uuid.uuid4()),
            "camera_id": "cam-01",
            "seq": i + 1,
            "timestamp": time.time(),
            "width": frame.shape[1],
            "height": frame.shape[0],
            "image_b64": encode_frame(frame),
        }

        # ---- mô phỏng Processing Server: decode + detect ----
        decoded = decode_frame(camera_msg["image_b64"])
        boxes = detector.detect(decoded)

        detection_result = {
            "frame_id": camera_msg["frame_id"],
            "camera_id": camera_msg["camera_id"],
            "seq": camera_msg["seq"],
            "captured_at": camera_msg["timestamp"],
            "processed_at": time.time(),
            "inference_ms": round(np.random.uniform(15, 45), 1),
            "width": camera_msg["width"],
            "height": camera_msg["height"],
            "people_count": len(boxes),
            "boxes": boxes,
        }

        # ---- mô phỏng Storage Server: thêm trường lưu trữ ----
        detection_result["captured_at_iso"] = datetime.fromtimestamp(
            detection_result["captured_at"], tz=timezone.utc
        ).isoformat()
        detection_result["inserted_at"] = datetime.now(tz=timezone.utc).isoformat()

        results.append(detection_result)
        time.sleep(0.05)

    out_dir = os.path.join(os.path.dirname(__file__), "..", "results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "sample_output.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Da sinh {len(results)} ban ghi mau -> {out_path}")
    print(f"Tong so nguoi dem duoc qua {num_frames} frame: {sum(r['people_count'] for r in results)}")


if __name__ == "__main__":
    run()
