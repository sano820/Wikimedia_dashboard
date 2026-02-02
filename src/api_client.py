"""
Wikimedia EventStreams API 클라이언트 (SSE)
- recentchange 스트림에서 이벤트(dict)를 계속 yield
"""
import json
import os
import requests
from dotenv import load_dotenv

# .env에 저장된 환경변수 로드
load_dotenv()

# Wikimedia EventStreams API
STREAM_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
# Wikimedia User-Agent 명시(정책상 필요)
USER_AGENT = os.getenv("WIKI_USER_AGENT", "Wikimeda_Dashboard/0.1 (contact: you@example.com)")


def iter_recentchange_events():
    """
    Wikimedia recentchange SSE 스트림에 역렬해 발생하는 이벤트를 무한히 하나씩 yield 하는 제너레이터 함수
    recentchange : 최근에 변경된 이벤트
    yield : 이벤트 발생할 때마다 값 하나씩 반환
    제너레이터 함수 : yeild 키워드 포함한 함수
    """
    # http 요청 헤더 설정
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": USER_AGENT,  # 403 차단 방지에 중요
    }

    # 400번대, 500번대 에러 발생 시 로그 확인하기 위한 try-except 구문
    try :
        # timeout: (connect_timeout, read_timeout)
        with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as r:
            '''
            args:
                STREAM_URL : Wikimedia API 주소
                headers= headers :  요청 해더
                stream = true : 응답 실시간 수신
                timeout (a,b) : a > wikimedia api 연결 기다리는 최대 대기 시간   // b > wikimedia에서 발생한 다음 변경 이벤트 기다리는 시간
            '''
            r.raise_for_status()

            # SSE 스트림에서 한 줄씩 데이터 읽음(\n 기준)
            # decode_unicode=True : bytes가 아닌 str로 변환해서 반환
            # startswith("data:"), json.loads() 쓰려면 str 형태여야 함
            for line in r.iter_lines(decode_unicode=True):

                # keep-alive 용 빈줄(연결이 끊기지 않게끔 가끔 아무 내용 없는 줄 보냄)
                if not line:
                    continue

                # SSE는 보통 "data: {...}" 형태로 전달됨
                if line.startswith("data:"):
                    payload = line[len("data:"):].strip()  # "data:" 제거 후 json 문자열만 추출

                    if not payload:
                        continue
                    
                    try:
                        yield json.loads(payload)  # json을 dict 변환 후 yeild(이벤트(dict) 1개 단위로 반환)

                    except json.JSONDecodeError: # json 파싱 실패한 데이터 건너뜀
                        continue
    # http 상태 코드 기반 에러(403,404, 500 등)
    except requests.exceptions.HTTPError as e:
        print("HTTP 에러:", e)
    # 네트워크 , 타임아웃, 연결 실패 등
    except requests.exceptions.RequestException as e:
        print("네트워크 에러:", e)