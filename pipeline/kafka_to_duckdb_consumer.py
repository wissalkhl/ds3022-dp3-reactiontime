import json
from datetime import datetime

from confluent_kafka import Consumer
import duckdb

DB_PATH = "data/db/reaction.duckdb"


def create_schema(conn: duckdb.DuckDBPyConnection):
    with open("pipeline/schema.sql") as f:
        sql = f.read()
    conn.execute(sql)


def parse_ts(x):
    """
    x can be:
    - unix epoch (int/float) -> Wikipedia
    - ISO8601 string        -> Bluesky
    """
    if isinstance(x, (int, float)):
        return datetime.utcfromtimestamp(x)
    if isinstance(x, str):
        try:
            
            return datetime.fromisoformat(x.replace("Z", "+00:00"))
        except Exception:
            pass
    # fallback if weird
    return datetime.utcnow()


def main():
    conn = duckdb.connect(DB_PATH)
    create_schema(conn)

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "reaction-consumer",
            "auto.offset.reset": "earliest",
        }
    )

    consumer.subscribe(["social-posts", "wiki-edits"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            topic_name = msg.topic()
            payload = json.loads(msg.value().decode("utf-8"))

            if topic_name == "social-posts":
                conn.execute(
                    """
                    INSERT INTO social_events
                    (id, author, handle, text, created_at, uri, sentiment, topic)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
                    """,
                    (
                        payload["id"],
                        payload["author"],
                        payload["handle"],
                        payload["text"],
                        parse_ts(payload["created_at"]),
                        payload["uri"],
                        payload["topic"],
                    ),
                )

            elif topic_name == "wiki-edits":
                conn.execute(
                    """
                    INSERT INTO wiki_events
                    (id, page_title, comment, namespace, is_bot, is_minor,
                     timestamp, user_text, topic)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["id"],
                        payload["page_title"],
                        payload["comment"],
                        payload["namespace"],
                        payload["is_bot"],
                        payload["is_minor"],
                        parse_ts(payload["timestamp"]),
                        payload["user_text"],
                        payload["topic"],
                    ),
                )

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
