# # src/producer.py
# """
# Producer
# - Wikimedia EventStreams에서 이벤트를 받아 Kafka topic에 JSON(bytes)으로 전송
# """
# import json
# import os
# import time
# from confluent_kafka import Producer
# from dotenv import load_dotenv

# from api_client import iter_recentchange_events

# load_dotenv()

# KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# TOPIC = os.getenv("KAFKA_TOPIC", "wiki-events")
# CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "wikimeda-producer")


# def main():
#     producer = Producer(
#         {
#             "bootstrap.servers": KAFKA_BOOTSTRAP,
#             "client.id": CLIENT_ID,
#             "acks": "all",
#             "retries": 10,
#             "linger.ms": 50,
#         }
#     )

#     print(f"[producer] bootstrap={KAFKA_BOOTSTRAP}, topic={TOPIC}")

#     sent = 0
#     try:
#         for event in iter_recentchange_events():
#             # 키는 있으면 좋음(파티셔닝/순서). 없으면 None.
#             key = (
#                 (event.get("wiki") or event.get("title")).encode("utf-8")
#                 if (event.get("wiki") or event.get("title"))
#                 else None
#             )

#             # dict -> JSON bytes
#             value = json.dumps(event, ensure_ascii=False).encode("utf-8")

#             # confluent_kafka는 send()가 아니라 produce()
#             producer.produce(TOPIC, key=key, value=value)

#             # 비동기 이벤트 처리 (가볍게)
#             producer.poll(0)

#             sent += 1
#             if sent % 100 == 0:
#                 # 주기적으로 flush해서 큐 적체/유실 위험 줄이기
#                 producer.flush(5)
#                 print(f"[producer] sent={sent}")

#     except KeyboardInterrupt:
#         print("[producer] stopped by user")
#     except Exception as e:
#         print(f"[producer] error: {e}")
#         time.sleep(2)
#     finally:
#         # 종료 전 남은 메시지 처리
#         producer.flush(10)


# if __name__ == "__main__":
#     main()


# src/producer.py
"""
Producer
- Wikimedia EventStreams에서 이벤트를 받아 Kafka topic에 JSON(bytes)으로 전송
- recentchange + revision-create를 한 URL에서 동시에 받아,
  이벤트의 meta.stream 기준으로 토픽을 분리해서 저장
"""
import json
import os
import time
from confluent_kafka import Producer
from dotenv import load_dotenv

from api_client import iter_wikimedia_events, WikimediaStreamError

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# ✅ 스트림별 토픽 분리 (env로 관리 권장)
TOPIC_RECENTCHANGE = os.getenv("KAFKA_TOPIC_RECENTCHANGE", "wiki-recentchange")
TOPIC_REVISION_CREATE = os.getenv("KAFKA_TOPIC_REVISION_CREATE", "wiki-revision-create")

# ✅ unknown 토픽: 예상 못한 이벤트(분류 실패/스키마 변경/설정 오류 등) 방어용 안전망
TOPIC_UNKNOWN = os.getenv("KAFKA_TOPIC_UNKNOWN", "wiki-unknown")

CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "wikimeda-producer")


def _select_topic(event: dict) -> str:
    """
    이벤트 dict에서 meta.stream을 보고 Kafka topic을 선택

    args:
        event (dict): Wikimedia SSE에서 받은 이벤트 JSON(dict)

    returns:
        str: 해당 이벤트를 저장할 Kafka topic 이름
    """
    stream_name = (event.get("meta") or {}).get("stream")

    if stream_name == "mediawiki.recentchange":
        return TOPIC_RECENTCHANGE
    if stream_name == "mediawiki.revision-create":
        return TOPIC_REVISION_CREATE
    return TOPIC_UNKNOWN


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

    print(
        f"[producer] bootstrap={KAFKA_BOOTSTRAP}, "
        f"topic_recentchange={TOPIC_RECENTCHANGE}, "
        f"topic_revision_create={TOPIC_REVISION_CREATE}, "
        f"topic_unknown={TOPIC_UNKNOWN}"
    )

    sent = 0

    # ✅ flush 튜닝
    # - 두 스트림을 동시에 받으면 이벤트 양이 증가
    # - flush를 너무 자주하면 성능 손해(동기 블로킹이 발생할 수 있음)
    # - 너무 안 하면 큐 적체 가능
    #
    # 기본값: 300건마다 flush(5초)
    FLUSH_EVERY = int(os.getenv("KAFKA_FLUSH_EVERY", "300"))
    FLUSH_TIMEOUT_SEC = int(os.getenv("KAFKA_FLUSH_TIMEOUT_SEC", "5"))

    try:
        for event in iter_wikimedia_events():
            # ✅ 스트림별 토픽 선택
            topic = _select_topic(event)

            # dict -> JSON bytes (Spark에서 필요한 정보 추출할 예정이므로 raw 저장)
            value = json.dumps(event, ensure_ascii=False).encode("utf-8")

            # ✅ key 제거 (옵션 A)
            producer.produce(topic, value=value)

            # 비동기 이벤트 처리 (가볍게)
            producer.poll(0)

            sent += 1
            if sent % FLUSH_EVERY == 0:
                producer.flush(FLUSH_TIMEOUT_SEC)
                print(f"[producer] sent={sent}")

    except KeyboardInterrupt:
        print("[producer] stopped by user")

    except WikimediaStreamError as e:
        # api_client에서 정의한 HTTP/Network 에러
        print(f"[producer] stream error: {e}")
        # 잠깐 대기 후 재시작 유도(일시 장애 대비)
        time.sleep(3)

    except Exception as e:
        print(f"[producer] error: {e}")
        time.sleep(3)

    finally:
        # 종료 전 남은 메시지 처리
        producer.flush(10)


if __name__ == "__main__":
    main()
