# src/consumer.py
"""
Consumer
- Kafka topic에서 메시지를 읽어 파일로 저장(JSON Lines)
"""
import json
import os
from datetime import datetime
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wikimeda-consumer-group")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "Wikimeda_Dashboard/data")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "")  # 비우면 자동 파일명 생성


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    out_path = OUTPUT_FILE.strip()
    if not out_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(OUTPUT_DIR, f"recentchange_{ts}.jsonl")

    # ✅ consumer 생성은 out_path 여부와 무관하게 항상 실행되어야 함
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "latest",  # 처음 실행 시 최신부터
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe([TOPIC])

    print(f"[consumer] bootstrap={KAFKA_BOOTSTRAP}, topic={TOPIC}, group={GROUP_ID}")
    print(f"[consumer] writing to {out_path}")

    count = 0
    try:
        with open(out_path, "a", encoding="utf-8") as f:
            while True:
                msg = consumer.poll(1.0)  # 1초 대기
                if msg is None:
                    continue

                if msg.error():
                    print(f"[consumer] error: {msg.error()}")
                    continue

                # ✅ JSON bytes → dict
                event = json.loads(msg.value().decode("utf-8"))

                f.write(json.dumps(event, ensure_ascii=False) + "\n")
                count += 1

                if count % 100 == 0:
                    f.flush()
                    print(f"[consumer] saved={count}")

    except KeyboardInterrupt:
        print("[consumer] stopped by user")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
