# Wikimeda_Dashboard
<img width="1166" height="750" alt="image" src="https://github.com/user-attachments/assets/33d58364-059c-4607-b097-bad23b205b98" />


  
## 프로젝트 소개
Wikimedia 실시간 스트림을 Kafka-Spark-Elasticsearch 로 처리하여,  
1~5분 윈도우 기반의 편집 트렌드/봇 비율/키워드 급증(이상치) 등을  
실시간 대시보드로 제공하는 스트리밍 데이터 파이프라인 구축


  
## 팀원 구성
|이재원|이시현|박상선|
|:---:|:---:|:---:|
|<img width="125" height="185" alt="image" src="https://github.com/user-attachments/assets/b106cee0-35a8-482f-b376-59136e0501a4" />|<img width="125" height="185" alt="image" src="https://github.com/user-attachments/assets/fde339f3-1000-4f31-8184-fd47cebad080" />|<img width="125" height="185" alt="image" src="https://github.com/user-attachments/assets/c7740be1-248f-40c4-9b29-99fe70f597b4" />|
| [@leo771331](https://github.com/leo771331) | [@sion2058](https://github.com/sion2058) | [@sano820](https://github.com/sano820) |



## 1. 개발 환경

- Data engineer : Kafka + Spark 
- Front-end : Node.js
- Back-end : Fastapi
- DB : Redis
- 협업 툴 : Discord, Notion


  
## 2. Key Design Decisions
1. Kafka 기반 수집 계층 분리
     - Producer와 처리 로직을 완전히 분리
     - 이벤트 버퍼링을 통한 지연 완화
     - 멀티 스트림 확장 가능 구조

2. Spark에서 계산, Backend는 전달만
     - Spark에서 모든 집계 수행
     - Backend는 Redis 조회 및 응답 포맷 통합
     - Stateless API 설계

3. Redis를 Serving Layer로 사용
     - 실시간 대시보드 특성상 낮은 지연이 핵심
     - 집계 결과만 저장
     - metrics:{stream}:{metric} 네임스페이스 전략 적용

4. 단일 API 인터페이스
     - /api/dashboard/latest
     - Frontend는 내부 파이프라인을 알 필요 없음
     - 계층 간 결합도 최소화


  
## 3. 프로젝트 구조
```
Wikimedia_dashboard
├── README.md
├── .gitignore
├── requirements.txt
├── src
│    ├── api_client.py
│    ├── consumer.py
│    ├── producer.py
│    └── spark_streaming.py
├── docker/
├── backend/
└── frontend/
```



## 4. 트러블 슈팅
- 링크 넣기


  
## 5. 프로젝트 실행하기
- Step1. docker 파일 내에서
```
docker compose up --build
```

- Step2. fontend 파일 내에서
```
npm run dev
```


  
## 6. [Core Insight]
실시간 데이터 시스템에서 중요한 것은 빠르게 계산하는 것이 아니라,  
계산과 조회를 분리해 빠르게 보여줄 수 있는 구조를 만드는 것이다.

