/*
- Vite 개발 서버 및 빌드 동작을 설정하는 파일
- frontend 개발 환경의 “인프라 설정”에 해당
*/

import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  // react 관련 기능 활성화  
  //jsx 문법을 부라우저가 이해할 수 있는 js로 변환
  plugins: [react()],  // 코드 수정시 화면이 즉시 갱신되는 개발 환경 제공
  server: {
    host: '0.0.0.0',
    port: 3000,
    /*
    proxy를 설정 => 개발환경에서 frontend와 backend를 하나의 서버처럼 사용하기 위함.
    */
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
