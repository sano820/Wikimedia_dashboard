# Wikimeda_Dashboard
새싹 2차 프로젝트


# notion link
https://www.notion.so/SESAC-2-2f65e6ec40c881c0b899f7c60fe51d78

## 프로젝트 소개
---
- Wikimedia Streaming data를 이용해서


## 팀원 구성
---
|이재원|박상선|이시현|
|---|---|---|
|@leo771331|@sano820|@sion2058|

### 1. 개발 환경
---
- Data Pipeline : Kafka + Spark 
- Front : Node.js
- Backe-end : Fastapi
- DB : Redis
- 협업 툴 : Discord, Notion

### 2. 
---
- 아키텍쳐 설명하기


### 3. 프로젝트 구조
---
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
├── docker
│    ├── compose.yml
│    ├── dockerfile
│    ├── dockerfile.spark
│    └── dockerfile.backend
├── backend
│    ├── __init__.py
│    ├── api_server.py
│    └── requirements.txt
└── frontend
     ├── App.jsx
```

