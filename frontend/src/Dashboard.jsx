import { useMemo, useState } from "react";
import useDashboardData from "./hooks/useDashboardData";

// 시각화 컴포넌트들 (표현 담당)
import TotalEventsChart from "./components/TotalEventsChart";
import EventTypePieChart from "./components/EventTypePieChart";
import BotRatioGauge from "./components/BotRatioGauge";
import TopWikiChart from "./components/TopWikiChart";
import TimeRangeSelector from "./components/TimeRangeSelector";
import EventsByTypeChart from "./components/EventsByTypeChart";
import ContentChangeGauge from "./components/ContentChangeGauge";


// epoch seconds(초) -> "mm:ss" 라벨로 변환 (차트 X축 라벨용)
function epochToLabel(t) {
  const d = new Date(t * 1000); // JS Date는 ms 단위라서 *1000
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${mm}:${ss}`;
}

// 에러 메시지 표시 컴포넌트 (error가 있을 때만 렌더링됨)
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

// 로딩 상태 표시 컴포넌트 (loading 중이고 data가 아직 없을 때 표시)
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

// 사용자가 선택할 시간 범위 옵션
// ⚠️ 현재 로직에서는 "최근 N분"이라기보다 "최근 N개 포인트"처럼 사용됨(slice(-N))
const TIME_RANGE_OPTIONS = [
  { value: 60, label: "최근 10분" },
  { value: 180, label: "최근 30분" },
  { value: 360, label: "최근 1시간" },
  { value: 720, label: "최근 2시간" },
  { value: 0, label: "전체" }
];

export default function Dashboard() {
  // 데이터 로딩(=API 폴링) 책임은 훅이 담당
  const { data, error, loading, lastUpdated } = useDashboardData();

  // 사용자가 선택한 시간 범위 상태 (기본값 360)
  const [timeRange, setTimeRange] = useState(360);

  // 전체 이벤트 시계열을 차트 입력 형태로 변환 + timeRange 필터 적용
  const totalSeries = useMemo(() => {
    if (!data?.total) return [];

    // data.total: [{t, v}, ...] 형태를 기대
    const series = data.total.map(p => ({
      t: p.t,                       // epoch seconds
      label: epochToLabel(p.t),     // x축 라벨(mm:ss)
      v: p.v                        // y값
    }));

    // timeRange가 0이면 전체, 아니면 마지막 N개만
    return timeRange > 0 ? series.slice(-timeRange) : series;
  }, [data, timeRange]);

  // 이벤트 타입별로 timeRange 구간 합계를 구해서 파이차트 입력 형태로 변환
  const typePieData = useMemo(() => {
    if (!data?.byType) return [];

    const typeTotals = {};

    // data.byType: { type: [{t, v}, ...], ... } 형태를 기대
    Object.entries(data.byType).forEach(([type, arr]) => {
      // 최근 N개만 사용할지(전체면 그대로)
      const filteredArr = timeRange > 0 ? (arr || []).slice(-timeRange) : (arr || []);

      // 타입별 v 합산
      const total = filteredArr.reduce((sum, p) => sum + (p.v || 0), 0);

      // 0인 타입은 제외
      if (total > 0) {
        typeTotals[type] = total;
      }
    });

    // recharts Pie 차트 입력 형태: [{name, value}, ...]
    return Object.entries(typeTotals)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value);
  }, [data, timeRange]);

  // Top Wiki 랭킹 데이터(거의 형태 유지)
  const topWiki = useMemo(() => {
    if (!data?.topWiki) return [];
    return data.topWiki.map(x => ({ k: x.k, v: x.v }));
  }, [data]);
    const typeKeys = useMemo(() => {
    if (!data?.byType) return [];
    // 너무 많으면 난잡하니 상위 몇 개만 (필요시 조정)
    return Object.keys(data.byType).slice(0, 8);
  }, [data]);

  const byTypeStackRows = useMemo(() => {
    if (!data?.byType) return [];

    // 모든 타입들의 timestamp 모으기
    const tsSet = new Set();
    for (const arr of Object.values(data.byType)) {
      for (const p of (arr || [])) tsSet.add(p.t);
    }

    const tsList = Array.from(tsSet).sort((a, b) => a - b);

    // timeRange 적용 (0이면 전체, 아니면 최근 N개)
    const filteredTs = timeRange > 0 ? tsList.slice(-timeRange) : tsList;

    // row 생성
    const rows = filteredTs.map((t) => {
      const d = new Date(t * 1000);
      const mm = String(d.getMinutes()).padStart(2, "0");
      const ss = String(d.getSeconds()).padStart(2, "0");
      return { t, label: `${mm}:${ss}` };
    });

    const rowsByTs = new Map(rows.map(r => [r.t, r]));

    // 각 타입을 row에 채우기
    for (const key of typeKeys) {
      const arr = data.byType[key] || [];
      for (const p of arr) {
        if (rowsByTs.has(p.t)) {
          rowsByTs.get(p.t)[key] = p.v;
        }
      }
    }

    // 누락값 0 처리
    for (const row of rows) {
      for (const key of typeKeys) {
        if (row[key] == null) row[key] = 0;
      }
    }

    return rows;
  }, [data, timeRange, typeKeys]);

  return (
    <div style={{
      maxWidth: 1400,
      margin: "0 auto",
      padding: "24px",
      minHeight: "100vh"
    }}>
      {/* 상단 헤더: 타이틀 + 설명 + 시간 범위 선택 + 마지막 업데이트 시간 */}
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

          {/* data가 있을 때는 refresh interval을 표시, 없으면 기본 설명 */}
          <p style={{
            color: "var(--color-text-secondary)",
            fontSize: 14,
            margin: 0
          }}>
            {data
              ? `실시간 스트리밍 데이터를 ${Math.floor((parseInt(import.meta.env.VITE_REFRESH_INTERVAL || "5000", 10)) / 1000)}초마다 갱신합니다.`
              : "Wikimedia 이벤트 스트림을 실시간으로 분석합니다."
            }
          </p>
        </div>

        <div style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "flex-end",
          gap: 8
        }}>
          {/* 시간 범위 선택 (select) */}
          <TimeRangeSelector
            value={timeRange}
            onChange={setTimeRange}
            options={TIME_RANGE_OPTIONS}
          />

          {/* 마지막 업데이트 시간 표시 */}
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

      {/* 에러가 있으면 경고 박스 표시 */}
      {error && <ErrorMessage message={error} />}

      {/* 로딩 중이고 data가 아직 없으면 로딩 화면 */}
      {loading && !data ? (
        <LoadingSpinner />
      ) : data ? (
        <>
          {/* 1행: 전체 이벤트 추이 + bot 비율 */}
          <div style={{
            display: "grid",
            gridTemplateColumns: "2fr 1fr 1fr",
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
            <ContentChangeGauge
            ratio={data.contentChange?.ratio}
            changedCount={data.contentChange?.changed}
            totalRevisionCount={data.contentChange?.total}
            windowSec={data.contentChange?.windowSec}
            />
          </div>

          {/* 2행: 타입별 파이차트 + top wiki */}
          <div style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: 20
          }}>
            <EventTypePieChart data={typePieData} />
            <TopWikiChart
              data={topWiki}
              windowMin={Math.floor((data.rangeMin || 5) / 60)}
            />
          </div>

          <div style={{ marginTop: 20 }}>
          <EventsByTypeChart data={byTypeStackRows} typeKeys={typeKeys} />
          </div>
        </>
      ) : null}
    </div>
  );
}
