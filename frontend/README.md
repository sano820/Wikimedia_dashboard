# Wikimedia Dashboard - Frontend

Wikimedia 실시간 스트리밍 데이터 대시보드의 프론트엔드 애플리케이션입니다.

## 기술 스택

- **React 19** - UI 라이브러리
- **Vite** - 빌드 도구 및 개발 서버
- **Recharts** - 차트 라이브러리

## 프로젝트 구조

```
frontend/
├── src/
│   ├── components/          # 재사용 가능한 컴포넌트
│   │   ├── Card.jsx
│   │   ├── BotRatioGauge.jsx
│   │   ├── TotalEventsChart.jsx
│   │   ├── EventsByTypeChart.jsx
│   │   └── TopWikiChart.jsx
│   ├── hooks/               # 커스텀 훅
│   │   └── useDashboardData.js
│   ├── App.jsx              # 메인 App 컴포넌트
│   ├── Dashboard.jsx        # 대시보드 메인 페이지
│   ├── main.jsx             # 엔트리 포인트
│   └── index.css            # 전역 스타일
├── public/                  # 정적 파일
├── index.html               # HTML 템플릿
├── vite.config.js           # Vite 설정
├── package.json
└── .env.example             # 환경변수 예제
```

## 시작하기

### 1. 의존성 설치

```bash
npm install
```

### 2. 환경변수 설정

`.env.example` 파일을 `.env`로 복사하고 필요한 값을 설정합니다.

```bash
cp .env.example .env
```

### 3. 개발 서버 실행

```bash
npm run dev
```

개발 서버가 http://localhost:3000 에서 실행됩니다.

### 4. 프로덕션 빌드

```bash
npm run build
```

빌드된 파일은 `dist/` 디렉토리에 생성됩니다.

### 5. 프로덕션 미리보기

```bash
npm run preview
```

## 주요 기능

### 대시보드 구성

1. **전체 이벤트량 차트** - 시간별 전체 이벤트 수를 라인 차트로 표시
2. **Bot 비율 게이지** - 전체 이벤트 중 봇 이벤트의 비율을 게이지로 표시
3. **이벤트 타입별 분포** - 타입별 이벤트 분포를 스택 영역 차트로 표시
4. **Top Wiki 차트** - 가장 활발한 위키 도메인을 바 차트로 표시

### 데이터 갱신

- 기본적으로 5초마다 백엔드 API에서 최신 데이터를 가져옵니다.
- 갱신 주기는 환경변수 `VITE_REFRESH_INTERVAL`로 조정할 수 있습니다.

## API 연동

프론트엔드는 백엔드 API의 `/api/dashboard/latest` 엔드포인트에서 데이터를 가져옵니다.

### 개발 환경
Vite 프록시를 통해 `http://localhost:8000`로 요청을 전달합니다.

### 프로덕션 환경
환경변수 `VITE_API_BASE_URL`에 백엔드 서버 URL을 설정합니다.

## 개발 가이드

### 컴포넌트 추가

새로운 차트나 컴포넌트를 추가할 때는 `src/components/` 디렉토리에 파일을 생성하고, `Dashboard.jsx`에서 import하여 사용합니다.

### 커스텀 훅 추가

데이터 fetching이나 상태 관리 로직은 `src/hooks/` 디렉토리에 커스텀 훅으로 분리합니다.

## 문제 해결

### API 연결 오류

1. 백엔드 서버가 실행 중인지 확인
2. `vite.config.js`의 프록시 설정 확인
3. 환경변수 설정 확인

### 차트가 표시되지 않음

1. 브라우저 콘솔에서 에러 메시지 확인
2. 백엔드 API 응답 데이터 형식 확인
3. Recharts 라이브러리가 제대로 설치되었는지 확인
