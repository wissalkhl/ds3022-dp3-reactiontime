import json
import requests
from confluent_kafka import Producer
import yaml

WIKI_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

producer = Producer({"bootstrap.servers": "localhost:29092"})

with open("config/topics.yaml") as f:
    config = yaml.safe_load(f)
TOPICS = config["topics"]


def detect_topic(title: str, comment: str):
    text = f"{title} {comment}".lower()
    for t in TOPICS:
        if t.lower() in text:
            return t
    return None


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    # Stream Server-Sent Events from Wikimedia
    with requests.get(WIKI_URL, stream=True) as r:
        for line in r.iter_lines():
            if not line:
                continue

            if not line.startswith(b"data: "):
                continue

            try:
                data = json.loads(line[len(b"data: "):])
            except json.JSONDecodeError:
                continue

            if data.get("type") != "edit":
                continue

            title = data.get("title", "") or ""
            comment = data.get("comment", "") or ""

            topic = detect_topic(title, comment)
            if topic is None:
                # skip events not related to our topics
                continue

            record = {
                "id": str(data.get("id")),
                "page_title": title,
                "comment": comment,
                "namespace": data.get("namespace"),
                "is_bot": data.get("bot", False),
                "is_minor": data.get("minor", False),
                "timestamp": data.get("timestamp"),  # unix epoch
                "user_text": data.get("user", ""),
                "topic": topic,
            }

            producer.produce(
                "wiki-edits",
                key=record["id"],
                value=json.dumps(record),
                callback=delivery_report,
            )
            producer.poll(0)

    producer.flush()


if __name__ == "__main__":
    main()
