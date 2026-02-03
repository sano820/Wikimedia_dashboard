import { useMemo, useState } from "react";
import useDashboardData from "./hooks/useDashboardData";
import TotalEventsChart from "./components/TotalEventsChart";
import EventTypePieChart from "./components/EventTypePieChart";
import BotRatioGauge from "./components/BotRatioGauge";
import TopWikiChart from "./components/TopWikiChart";
import TimeRangeSelector from "./components/TimeRangeSelector";

function epochToLabel(t) {
  const d = new Date(t * 1000);
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${mm}:${ss}`;
}

function ErrorMessage({ message }) {
  return (
    <div style={{
      marginTop: 16,
      padding: 16,
      borderRadius: 12,
      background: "#fff3cd",
      border: "1px solid #ffeeba",
      color: "#856404",
      fontSize: 14
    }}>
      {message}
    </div>
  );
}

function LoadingSpinner() {
  return (
    <div style={{
      display: "flex",
      justifyContent: "center",
      alignItems: "center",
      minHeight: "400px",
      color: "var(--color-text-secondary)"
    }}>
      <div>데이터 로딩 중...</div>
    </div>
  );
}

const TIME_RANGE_OPTIONS = [
  { value: 60, label: "최근 10분" },
  { value: 180, label: "최근 30분" },
  { value: 360, label: "최근 1시간" },
  { value: 720, label: "최근 2시간" },
  { value: 0, label: "전체" }
];

export default function Dashboard() {
  const { data, error, loading, lastUpdated } = useDashboardData();
  const [timeRange, setTimeRange] = useState(360); // 기본값: 1시간

  const totalSeries = useMemo(() => {
    if (!data?.total) return [];
    const series = data.total.map(p => ({
      t: p.t,
      label: epochToLabel(p.t),
      v: p.v
    }));
    // timeRange 적용 (0이면 전체)
    return timeRange > 0 ? series.slice(-timeRange) : series;
  }, [data, timeRange]);

  // 이벤트 타입별 전체 합계를 계산하여 파이 차트 데이터 생성
  const typePieData = useMemo(() => {
    if (!data?.byType) return [];

    // 각 타입별 총합 계산
    const typeTotals = {};
    Object.entries(data.byType).forEach(([type, arr]) => {
      // timeRange 적용: 최근 N개 데이터만 사용
      const filteredArr = timeRange > 0 ? (arr || []).slice(-timeRange) : (arr || []);
      const total = filteredArr.reduce((sum, p) => sum + (p.v || 0), 0);
      if (total > 0) {
        typeTotals[type] = total;
      }
    });

    // 파이 차트 형식으로 변환 [{name, value}]
    return Object.entries(typeTotals)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value); // 값이 큰 순서대로 정렬
  }, [data, timeRange]);

  const topWiki = useMemo(() => {
    if (!data?.topWiki) return [];
    return data.topWiki.map(x => ({ k: x.k, v: x.v }));
  }, [data]);

  return (
    <div style={{
      maxWidth: 1400,
      margin: "0 auto",
      padding: "24px",
      minHeight: "100vh"
    }}>
      <header style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "flex-start",
        marginBottom: 24
      }}>
        <div>
          <h1 style={{ fontSize: 32, marginBottom: 8 }}>
            Wikimedia Realtime Dashboard
          </h1>
          <p style={{
            color: "var(--color-text-secondary)",
            fontSize: 14,
            margin: 0
          }}>
            {data ? `실시간 스트리밍 데이터를 ${Math.floor((parseInt(import.meta.env.VITE_REFRESH_INTERVAL || "5000", 10)) / 1000)}초마다 갱신합니다.` : "Wikimedia 이벤트 스트림을 실시간으로 분석합니다."}
          </p>
        </div>
        <div style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "flex-end",
          gap: 8
        }}>
          <TimeRangeSelector
            value={timeRange}
            onChange={setTimeRange}
            options={TIME_RANGE_OPTIONS}
          />
          {lastUpdated && (
            <div style={{
              color: "var(--color-text-secondary)",
              fontSize: 13
            }}>
              최종 업데이트: {lastUpdated.toLocaleTimeString("ko-KR")}
            </div>
          )}
        </div>
      </header>

      {error && <ErrorMessage message={error} />}

      {loading && !data ? (
        <LoadingSpinner />
      ) : data ? (
        <>
          <div style={{
            display: "grid",
            gridTemplateColumns: "2fr 1fr",
            gap: 20,
            marginBottom: 20
          }}>
            <TotalEventsChart data={totalSeries} bucketSec={data.bucketSec} />
            <BotRatioGauge
              ratio={data.bot?.ratio}
              total={data.bot?.total}
              bot={data.bot?.bot}
              windowSec={data.bot?.windowSec}
            />
          </div>

          <div style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: 20
          }}>
            <EventTypePieChart data={typePieData} />
            <TopWikiChart data={topWiki} windowMin={Math.floor((data.rangeMin || 5) / 60)} />
          </div>
        </>
      ) : null}
    </div>
  );
}
