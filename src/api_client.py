# # src/api_client.py
# """
# Wikimedia EventStreams API 클라이언트 (SSE)
# - recentchange 스트림에서 이벤트(dict)를 계속 yield
# """
# import json
# import os
# import requests
# from dotenv import load_dotenv

# # .env에 저장된 환경변수 로드
# load_dotenv()

# # Wikimedia EventStreams API
# STREAM_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
# # Wikimedia User-Agent 명시(정책상 필요)
# USER_AGENT = os.getenv("WIKI_USER_AGENT", "Wikimeda_Dashboard/0.1 (contact: you@example.com)")


# def iter_recentchange_events():
#     """
#     Wikimedia recentchange SSE 스트림에 역렬해 발생하는 이벤트를 무한히 하나씩 yield 하는 제너레이터 함수
#     recentchange : 최근에 변경된 이벤트
#     yield : 이벤트 발생할 때마다 값 하나씩 반환
#     제너레이터 함수 : yeild 키워드 포함한 함수
#     """
#     # http 요청 헤더 설정
#     headers = {
#         "Accept": "text/event-stream",
#         "User-Agent": USER_AGENT,  # 403 차단 방지에 중요
#     }

#     # 400번대, 500번대 에러 발생 시 로그 확인하기 위한 try-except 구문
#     try :
#         # timeout: (connect_timeout, read_timeout)
#         with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as r:
#             '''
#             args:
#                 STREAM_URL : Wikimedia API 주소
#                 headers= headers :  요청 해더
#                 stream = true : 응답 실시간 수신
#                 timeout (a,b) : a > wikimedia api 연결 기다리는 최대 대기 시간   // b > wikimedia에서 발생한 다음 변경 이벤트 기다리는 시간
#             '''
#             r.raise_for_status()

#             # SSE 스트림에서 한 줄씩 데이터 읽음(\n 기준)
#             # decode_unicode=True : bytes가 아닌 str로 변환해서 반환
#             # startswith("data:"), json.loads() 쓰려면 str 형태여야 함
#             for line in r.iter_lines(decode_unicode=True):

#                 # keep-alive 용 빈줄(연결이 끊기지 않게끔 가끔 아무 내용 없는 줄 보냄)
#                 if not line:
#                     continue

#                 # SSE는 보통 "data: {...}" 형태로 전달됨
#                 if line.startswith("data:"):
#                     payload = line[len("data:"):].strip()  # "data:" 제거 후 json 문자열만 추출

#                     if not payload:
#                         continue
                    
#                     try:
#                         yield json.loads(payload)  # json을 dict 변환 후 yeild(이벤트(dict) 1개 단위로 반환)

#                     except json.JSONDecodeError: # json 파싱 실패한 데이터 건너뜀
#                         continue
#     # http 상태 코드 기반 에러(403,404, 500 등)
#     except requests.exceptions.HTTPError as e:
#         print("HTTP 에러:", e)
#     # 네트워크 , 타임아웃, 연결 실패 등
#     except requests.exceptions.RequestException as e:
#         print("네트워크 에러:", e)


# src/api_client.py
"""
Wikimedia EventStreams API 클라이언트 (SSE)
- recentchange + revision-create 스트림에서 이벤트(dict)를 계속 yield
- composite stream: /v2/stream/recentchange,revision-create 형태로 한 번에 수신
"""
import json
import os
import requests
from dotenv import load_dotenv

# .env에 저장된 환경변수 로드
load_dotenv()

# Wikimedia EventStreams API
# - 기본값: recentchange + revision-create 동시 구독
STREAM_URL = os.getenv(
    "WIKI_STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange,revision-create"
)

# Wikimedia User-Agent 명시(정책상 필요)
USER_AGENT = os.getenv("WIKI_USER_AGENT", "Wikimeda_Dashboard/0.1 (contact: you@example.com)")


# -------------------------
# ✅ 에러 정의 (에러를 종류별로 분리해서 보기 좋게)
# -------------------------
class WikimediaStreamError(Exception):
    """Wikimedia SSE 스트림 처리 중 발생하는 공통 예외"""


class WikimediaHTTPError(WikimediaStreamError):
    """HTTP 상태코드 기반 예외 (403/404/500 등)"""


class WikimediaNetworkError(WikimediaStreamError):
    """네트워크/타임아웃/연결 실패 등 요청 레벨 예외"""


def iter_wikimedia_events():
    """
    Wikimedia SSE 스트림에 연결해 발생하는 이벤트를 무한히 하나씩 yield 하는 제너레이터 함수

    yield:
        dict: Wikimedia 이벤트 JSON(dict) 1개 단위로 반환

    동작 방식(중요):
        SSE는 한 이벤트가 여러 라인(event/id/data)로 올 수 있고,
        "빈 줄"이 이벤트의 끝을 의미함.
        따라서 data: 라인을 모아서(버퍼링) 이벤트 끝에서 JSON 파싱함.
    """
    # http 요청 헤더 설정
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": USER_AGENT,  # 403 차단 방지에 중요
    }

    # timeout: (connect_timeout, read_timeout)
    connect_timeout = int(os.getenv("WIKI_CONNECT_TIMEOUT", "10"))
    read_timeout = int(os.getenv("WIKI_READ_TIMEOUT", "60"))

    # 400번대, 500번대 에러 발생 시 로그 확인하기 위한 try-except 구문
    try:
        with requests.get(
            STREAM_URL,
            headers=headers,
            stream=True,
            timeout=(connect_timeout, read_timeout)
        ) as r:
            """
            args:
                STREAM_URL : Wikimedia API 주소 (복수 스트림일 경우 콤마로 나열)
                headers= headers :  요청 헤더
                stream = true : 응답 실시간 수신
                timeout (a,b) : a > wikimedia api 연결 기다리는 최대 대기 시간
                               b > wikimedia에서 발생한 다음 변경 이벤트 기다리는 시간
            """
            r.raise_for_status()

            # SSE 필드 버퍼
            event_type = None
            event_id = None
            data_lines = []

            # SSE 스트림에서 한 줄씩 데이터 읽음(\n 기준)
            # decode_unicode=True : bytes가 아닌 str로 변환해서 반환
            for line in r.iter_lines(decode_unicode=True):

                # None 방어
                if line is None:
                    continue

                line = line.strip()

                # ✅ SSE 이벤트 경계(빈 줄)
                # - keep-alive 빈 줄이 아니라 "이벤트 끝"을 의미하는 빈 줄도 존재
                # - 여기서 data_lines를 모아서 JSON 파싱
                if line == "":
                    if data_lines:
                        payload = "\n".join(data_lines).strip()
                        data_lines = []

                        if not payload:
                            # payload 비어있으면 그냥 다음 이벤트로
                            event_type = None
                            event_id = None
                            continue

                        try:
                            # ✅ JSON 파싱 성공 -> 이벤트 1건 yield
                            yield json.loads(payload)

                        except json.JSONDecodeError as e:
                            # ✅ 운영상: JSON 한 건 깨졌다고 스트림 전체를 끊지 않기 위해
                            # raise 하지 않고 해당 이벤트만 skip
                            print(f"[api_client] JSON 파싱 실패(스킵): {e}")
                            # 필요하면 일부만 확인 (너무 길면 로그 폭발 방지)
                            # print(payload[:300])
                            # 다음 이벤트 계속 받기
                            event_type = None
                            event_id = None
                            continue

                    # 다음 이벤트를 위해 초기화
                    event_type = None
                    event_id = None
                    continue

                # SSE event / id / data 라인 처리
                if line.startswith("event:"):
                    event_type = line[len("event:"):].strip()
                    continue

                if line.startswith("id:"):
                    # composite stream에서는 id가 배열(JSON) 문자열로 올 수 있음
                    # (재연결 커서로 활용할 수 있음)
                    event_id = line[len("id:"):].strip()
                    continue

                if line.startswith("data:"):
                    payload_part = line[len("data:"):].strip()
                    if payload_part:
                        data_lines.append(payload_part)
                    continue

                # (예외 케이스 방어)
                # 일반적으로 Wikimedia는 data: 로 오지만, 라인이 이어지는 케이스가 있으면 아래 사용
                # data_lines.append(line)

    # http 상태 코드 기반 에러(403,404, 500 등)
    except requests.exceptions.HTTPError as e:
        print("HTTP 에러:", e)
        raise WikimediaHTTPError(str(e)) from e

    # 네트워크 , 타임아웃, 연결 실패 등
    except requests.exceptions.RequestException as e:
        print("네트워크 에러:", e)
        raise WikimediaNetworkError(str(e)) from e
