import asyncio
import json

import websockets
import cbor2
from confluent_kafka import Producer
import yaml

producer = Producer({"bootstrap.servers": "localhost:29092"})

with open("config/topics.yaml") as f:
    config = yaml.safe_load(f)
TOPICS = config["topics"]


def detect_topic(text: str):
    t = text.lower()
    for topic in TOPICS:
        if topic.lower() in t:
            return topic
    return None


async def consume_firehose():
    # Bluesky firehose endpoint used in a lot of examples
    url = "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"

    async with websockets.connect(url) as ws:
        async for message in ws:
            try:
                frame = cbor2.loads(message)
            except Exception:
                continue

            commit = frame.get("commit") or frame.get("body") or {}
            ops = commit.get("ops", [])
            repo = commit.get("repo", "")
            time_str = commit.get("time")  # ISO8601 string

            for op in ops:
                if op.get("action") != "create":
                    continue

                path = op.get("path", "")
                # Heuristic: post paths usually start with app.bsky.feed.post
                if not path.startswith("app.bsky.feed.post"):
                    continue

                record = op.get("record", {})
                text = record.get("text", "")
                topic = detect_topic(text)
                if topic is None:
                    continue

                uri = f"at://{repo}/{path}"

                event = {
                    "id": uri,
                    "author": repo,
                    "handle": repo,  # if you decode separate handle, put it here
                    "text": text,
                    "created_at": time_str,
                    "uri": uri,
                    "topic": topic,
                }

                producer.produce(
                    "social-posts",
                    key=event["id"],
                    value=json.dumps(event),
                )
                producer.poll(0)


def main():
    asyncio.run(consume_firehose())


if __name__ == "__main__":
    main()
