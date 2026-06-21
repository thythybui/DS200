import os
import json
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [STORAGE] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "detection-results")
GROUP_ID = os.getenv("CONSUMER_GROUP", "storage-server-group")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "people_counting")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "detections")


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


def ensure_indexes(collection):
    collection.create_index("camera_id")
    collection.create_index("captured_at")
    collection.create_index([("camera_id", 1), ("captured_at", -1)])


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    ensure_indexes(collection)

    consumer = build_consumer()
    logger.info("Lang nghe topic '%s', luu vao MongoDB %s.%s", INPUT_TOPIC, MONGO_DB, MONGO_COLLECTION)

    stored = 0
    for message in consumer:
        doc = message.value
        doc["captured_at_iso"] = datetime.fromtimestamp(doc["captured_at"], tz=timezone.utc).isoformat()
        doc["inserted_at"] = datetime.now(tz=timezone.utc).isoformat()

        collection.insert_one(doc)
        stored += 1

        if stored % 20 == 0:
            logger.info(
                "Da luu %d ban ghi | camera=%s | people_count=%d",
                stored, doc.get("camera_id"), doc.get("people_count"),
            )


if __name__ == "__main__":
    main()
