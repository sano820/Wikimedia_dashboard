import { useEffect, useState, useCallback } from "react";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "";
const REFRESH_INTERVAL = parseInt(import.meta.env.VITE_REFRESH_INTERVAL || "5000", 10);
const CHART_DATA_LIMIT = parseInt(import.meta.env.VITE_CHART_DATA_LIMIT || "0", 10);

export default function useDashboardData() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState(null);

  const fetchLatest = useCallback(async () => {
    try {
      // limit 파라미터 추가 (0이면 쿼리에 포함하지 않음)
      const url = new URL(`${API_BASE_URL}/api/dashboard/latest`, window.location.origin);
      if (CHART_DATA_LIMIT > 0) {
        url.searchParams.set('limit', CHART_DATA_LIMIT);
      }

      const res = await fetch(url.toString());

      if (res.status === 204) {
        setError("아직 집계 데이터가 없습니다 (Spark가 Redis에 쓰기 전)");
        setData(null);
        return;
      }

      if (!res.ok) {
        throw new Error(`API 에러: ${res.status} ${res.statusText}`);
      }

      const json = await res.json();
      setData(json);
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
