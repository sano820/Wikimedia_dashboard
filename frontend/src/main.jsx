// React 앱을 브라우저 DOM에 연결하고 렌더링을 시작하는 진입 파일

import { StrictMode } from 'react' // 개발단계에서 잠재적인 문제를 미리 집기 위한 도구 => 일부 라이프사이클 로직을 의도적으로 두번 실행해 문제 코드 탐지
import { createRoot } from 'react-dom/client'  
import './index.css'
import App from './App.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
