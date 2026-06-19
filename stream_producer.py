import json
import requests
from confluent_kafka import Producer
import time
import os
from dotenv import load_dotenv

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "wiki_raw_stream"

load_dotenv()
user_agent = os.getenv("USER_AGENT")


def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver: {err}")
    # else:
    #     print(f"Delivered to {msg.topic()} [partition{msg.partition()}]")


def pull():
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    headers = {"User-Agent": user_agent}

    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    print("Staring to pull...")

    msg_count = 0
    while True:
        try:
            response = requests.get(url, stream=True, headers=headers)
            response.raise_for_status()

            for line in response.iter_lines():
                if line:
                    if line.startswith(b"data: "):
                        json_str = line[6:].decode("utf-8")
                        try:
                            data = json.loads(json_str)
                            if (
                                data.get("server_name")
                                and "wikipedia.org" in data["server_name"]
                                and data.get("type") == "edit"
                                and data.get("namespace") == 0
                            ):
                                p.produce(
                                    KAFKA_TOPIC,
                                    key=data["server_name"].encode("utf-8"),
                                    value=json.dumps(data).encode("utf-8"),
                                    callback=delivery_report,
                                )
                                p.poll(0)

                                msg_count += 1
                                if msg_count % 1 == 0:
                                    title = data.get("title", "No title")
                                    sever = data["server_name"]
                                    print(
                                        f"Send {msg_count} msg. Last msg: [{sever}] {title}"
                                    )
                        except json.JSONDecodeError:
                            continue

        except KeyboardInterrupt:
            print("Stopped manually")
            break
        except Exception as e:
            print(f"{e}, waiting 3seconds to reconnect")
            time.sleep(3)
    p.flush()
    print("Producer empty")


pull()
