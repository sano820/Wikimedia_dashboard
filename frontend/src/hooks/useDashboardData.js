// import { useEffect, useState, useCallback } from "react";

// // 환경변수 기반 설정
// // - VITE_API_BASE_URL: 프록시 쓰면 ""여도 되고, 직접 호출이면 "http://..." 형태 가능
// const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "";

// // - VITE_REFRESH_INTERVAL: 몇 ms마다 API를 다시 호출할지(폴링 주기)
// const REFRESH_INTERVAL = parseInt(import.meta.env.VITE_REFRESH_INTERVAL || "5000", 10);

// // - VITE_CHART_DATA_LIMIT: 서버에 limit 파라미터로 요청할지(0이면 미사용)
// const CHART_DATA_LIMIT = parseInt(import.meta.env.VITE_CHART_DATA_LIMIT || "0", 10);

// export default function useDashboardData() {
//   // 데이터/상태 관리
//   const [data, setData] = useState(null);         // 대시보드 데이터
//   const [error, setError] = useState(null);       // 에러 메시지
//   const [loading, setLoading] = useState(true);   // 로딩 여부
//   const [lastUpdated, setLastUpdated] = useState(null);   // 마지막 업데이트 시간(Date)

//   // 최신 데이터를 가져오는 함수 (useCallback으로 참조 안정화)
//   const fetchLatest = useCallback(async () => {
//     try {
//       // limit 파라미터 추가 (0이면 쿼리에 포함하지 않음)
//       // URL 객체로 안전하게 endpoint 생성
//       // window.location.origin을 base로 붙여서 상대/절대 경로 모두 안정적으로 처리      
//       const url = new URL(`${API_BASE_URL}/api/dashboard/latest`, window.location.origin);

//       // limit 파라미터는 0보다 클 때만 붙임
//       if (CHART_DATA_LIMIT > 0) {
//         url.searchParams.set('limit', CHART_DATA_LIMIT);
//       }

//       // API 호출
//       const res = await fetch(url.toString());

//       // 204: 데이터가 아직 없음을 의미하도록 서버가 설계한 케이스
//       if (res.status === 204) {
//         setError("아직 집계 데이터가 없습니다 (Spark가 Redis에 쓰기 전)");
//         setData(null);
//         return;
//       }

//       // 200~299가 아니면 에러 처리
//       if (!res.ok) {
//         throw new Error(`API 에러: ${res.status} ${res.statusText}`);
//       }

//       // 정상 응답 JSON 파싱
//       const json = await res.json();

//       // 상태 업데이트
//       setData(json);
//       setError(null);
//       setLastUpdated(new Date());
//     } catch (e) {
//       // fetch 실패, JSON 파싱 실패 등 예외 처리
//       setError(`데이터 로드 실패: ${e.message}`);
//       console.error("Dashboard fetch error:", e);
//     } finally {
//       // 최초 1회 로딩 종료 처리
//       setLoading(false);
//     }
//   }, []);

//   // 컴포넌트 마운트 시:
//   // - 즉시 1회 fetch
//   // - interval로 주기적 fetch
//   // - 언마운트 시 interval 정리  
//   useEffect(() => {
//     fetchLatest();
//     const intervalId = setInterval(fetchLatest, REFRESH_INTERVAL);
//     return () => clearInterval(intervalId);
//   }, [fetchLatest]);

//   // 외부에서 refresh를 수동 호출할 수 있게 fetchLatest도 함께 반환
//   return { data, error, loading, lastUpdated, refresh: fetchLatest };
// }
import { useEffect, useState, useCallback } from "react";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "";
const REFRESH_INTERVAL = parseInt(import.meta.env.VITE_REFRESH_INTERVAL || "5000", 10);
const CHART_DATA_LIMIT = parseInt(import.meta.env.VITE_CHART_DATA_LIMIT || "0", 10);

// "2026-..Z" 같은 ISO ts 문자열 -> epoch seconds(number)
function isoToEpochSec(ts) {
  const ms = Date.parse(ts);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null;
}

/**
 * 백엔드 응답(raw)이 metric1~5를 담고 온다고 가정하고,
 * 프론트에서 쓰는 표준 모델로 맞춘다.
 *
 * 기대(권장) raw 형태 예시:
 * {
 *   metric1: [{ts, count}, ...],
 *   metric2: [{ts, type_counts:{...}}, ...],
 *   metric3: {ts, bot_ratio, bot_count, total_count},
 *   metric4: [{domain, count}, ...],
 *   metric5: {ts, content_change_ratio, content_changed_count, total_revision_count},
 *   bucketSec: 10
 * }
 *
 * (혹시 키 이름이 다르면 아래 or 조건들에서 흡수하도록 해둠)
 */
function normalizeDashboard(raw) {
  if (!raw) return null;
  if (raw.total && raw.byType && raw.topWiki) {
    return raw;
  }

  // metric1: [{ts,count}] -> total: [{t,v}]
  const m1 = raw.metric1 || raw.events_total || raw.total;
  const total = Array.isArray(m1)
    ? m1.map(x => ({
        t: isoToEpochSec(x.ts ?? x.window_start ?? x.t),
        v: x.count ?? x.event_count ?? x.v ?? 0
      })).filter(p => p.t !== null)
    : [];

  // metric2: [{ts, type_counts:{...}}] -> byType: {type:[{t,v}]}
  const m2 = raw.metric2 || raw.events_by_type || raw.byTypeWindows;
  const byType = {};
  if (Array.isArray(m2)) {
    for (const w of m2) {
      const t = isoToEpochSec(w.ts);
      const counts = w.type_counts || w.typeCounts || {};
      if (t === null) continue;

      for (const [type, cnt] of Object.entries(counts)) {
        if (!byType[type]) byType[type] = [];
        byType[type].push({ t, v: Number(cnt) || 0 });
      }
    }
    for (const type of Object.keys(byType)) {
      byType[type].sort((a, b) => a.t - b.t);
    }
  }

  // metric3: bot ratio
  const m3 = raw.metric3 || raw.bot_ratio || raw.bot;
  const bot = m3 ? {
    ratio: m3.bot_ratio ?? m3.ratio ?? 0,
    bot: m3.bot_count ?? m3.bot ?? 0,
    total: m3.total_count ?? m3.total ?? 0,
    windowSec: m3.windowSec ?? 60
  } : null;

  // metric4: [{domain,count}] -> topWiki: [{k,v}]
  const m4 = raw.metric4 || raw.top_domains || raw.topWiki || raw.top_domain;
  const topWiki = Array.isArray(m4)
    ? m4.map(x => ({
        k: x.domain ?? x.k ?? "unknown",
        v: x.count ?? x.v ?? 0
      }))
    : [];

  // metric5: content change ratio
  const m5 = raw.metric5 || raw.content_change_ratio || raw.contentChange;
  const contentChange = m5 ? {
    ratio: m5.content_change_ratio ?? m5.ratio ?? 0,
    changed: m5.content_changed_count ?? m5.changed ?? 0,
    total: m5.total_revision_count ?? m5.total ?? 0,
    windowSec: m5.windowSec ?? 60
  } : null;

  return {
    bucketSec: raw.bucketSec ?? 10,
    rangeMin: raw.rangeMin ?? 300,
    total,
    byType,
    bot,
    topWiki,
    contentChange
  };
}

export default function useDashboardData() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);

  const fetchLatest = useCallback(async () => {
    try {
      const url = new URL(`${API_BASE_URL}/api/dashboard/latest`, window.location.origin);
      if (CHART_DATA_LIMIT > 0) url.searchParams.set("limit", CHART_DATA_LIMIT);

      const res = await fetch(url.toString());

      if (res.status === 204) {
        setError("아직 집계 데이터가 없습니다 (Spark가 Redis에 쓰기 전)");
        setData(null);
        return;
      }
      if (!res.ok) throw new Error(`API 에러: ${res.status} ${res.statusText}`);

      const raw = await res.json();
      const normalized = normalizeDashboard(raw);

      setData(normalized);
      setError(null);
      setLastUpdated(new Date());
    } catch (e) {
      setError(`데이터 로드 실패: ${e.message}`);
      console.error("Dashboard fetch error:", e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchLatest();
    const intervalId = setInterval(fetchLatest, REFRESH_INTERVAL);
    return () => clearInterval(intervalId);
  }, [fetchLatest]);

  return { data, error, loading, lastUpdated, refresh: fetchLatest };
}
