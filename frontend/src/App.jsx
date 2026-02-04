import Dashboard from "./Dashboard.jsx"; // 실제 화면(대시보드) 컴포넌트

// App은 최상위 컨테이너 역할
// 지금은 Dashboard만 렌더링하지만, 나중에 라우팅/전역 Provider 등을 넣기 좋은 위치
export default function App() {
  return <Dashboard />;
}

