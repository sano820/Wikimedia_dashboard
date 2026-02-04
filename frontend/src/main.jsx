import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'          // 전역 스타일(CSS 변수/기본 스타일)을 앱에 적용
import App from './App.jsx'   // 최상위 컴포넌트

// index.html의 <div id="root"></div>를 찾아 React 앱을 붙임
createRoot(document.getElementById('root')).render(
  // StrictMode: 개발 환경에서 잠재 문제를 더 잘 잡기 위한 래퍼(프로덕션 영향 거의 없음)
  <StrictMode>
    <App />
  </StrictMode>,
)

