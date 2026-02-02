# src/producer.py
"""
Producer
- Wikimedia EventStreams에서 이벤트를 받아 Kafka topic에 JSON(bytes)으로 전송
"""
import json
import os
import time
from confluent_kafka import Producer
from dotenv import load_dotenv

from api_client import iter_recentchange_events

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-events")
CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "wikimeda-producer")


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": CLIENT_ID,
            "acks": "all",
            "retries": 10,
            "linger.ms": 50,
        }
    )

    print(f"[producer] bootstrap={KAFKA_BOOTSTRAP}, topic={TOPIC}")

    sent = 0
    try:
        for event in iter_recentchange_events():
            # 키는 있으면 좋음(파티셔닝/순서). 없으면 None.
            key = (
                (event.get("wiki") or event.get("title")).encode("utf-8")
                if (event.get("wiki") or event.get("title"))
                else None
            )

            # dict -> JSON bytes
            value = json.dumps(event, ensure_ascii=False).encode("utf-8")

            # confluent_kafka는 send()가 아니라 produce()
            producer.produce(TOPIC, key=key, value=value)

            # 비동기 이벤트 처리 (가볍게)
            producer.poll(0)

            sent += 1
            if sent % 100 == 0:
                # 주기적으로 flush해서 큐 적체/유실 위험 줄이기
                producer.flush(5)
                print(f"[producer] sent={sent}")

    except KeyboardInterrupt:
        print("[producer] stopped by user")
    except Exception as e:
        print(f"[producer] error: {e}")
        time.sleep(2)
    finally:
        # 종료 전 남은 메시지 처리
        producer.flush(10)


if __name__ == "__main__":
    main()
