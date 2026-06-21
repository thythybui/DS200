"""
DASHBOARD (phụ trợ) - đọc dữ liệu đã lưu trong MongoDB và hiển thị
số lượng người theo thời gian thực để minh hoạ/kiểm tra kết quả hệ thống.
"""

import os
from flask import Flask, jsonify, render_template
from pymongo import MongoClient, DESCENDING

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "people_counting")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "detections")

app = Flask(__name__)
client = MongoClient(MONGO_URI)
collection = client[MONGO_DB][MONGO_COLLECTION]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/latest")
def latest():
    docs = list(collection.find({}, {"_id": 0}).sort("captured_at", DESCENDING).limit(50))
    docs.reverse()
    return jsonify(docs)


@app.route("/api/summary")
def summary():
    latest_doc = collection.find_one({}, {"_id": 0}, sort=[("captured_at", DESCENDING)])
    total_records = collection.count_documents({})
    max_doc = collection.find_one({}, {"_id": 0}, sort=[("people_count", DESCENDING)])
    return jsonify({
        "current_count": latest_doc["people_count"] if latest_doc else 0,
        "camera_id": latest_doc["camera_id"] if latest_doc else None,
        "total_records": total_records,
        "max_count": max_doc["people_count"] if max_doc else 0,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
