import { useEffect, useMemo, useState } from "react";
import {
  ResponsiveContainer,
  LineChart, Line,
  AreaChart, Area,
  BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend
} from "recharts";

function epochToLabel(t) {
  const d = new Date(t * 1000);
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${mm}:${ss}`;
}

function Card({ title, children }) {
  return (
    <div style={{
      border: "1px solid #e6e6e6",
      borderRadius: 14,
      padding: 16,
      background: "white"
    }}>
      <div style={{ fontWeight: 800, marginBottom: 10 }}>{title}</div>
      {children}
    </div>
  );
}

function Gauge({ ratio, total, bot }) {
  const pct = Math.round((ratio ?? 0) * 100);
  return (
    <Card title="Bot 비율 (최근 1분)">
      <div style={{ display: "flex", alignItems: "baseline", gap: 12 }}>
        <div style={{ fontSize: 44, fontWeight: 900 }}>{pct}%</div>
        <div style={{ color: "#666", fontSize: 13 }}>
          (bot {bot ?? 0} / total {total ?? 0})
        </div>
      </div>
      <div style={{ height: 10, background: "#eee", borderRadius: 999, overflow: "hidden", marginTop: 12 }}>
        <div style={{ width: `${pct}%`, height: "100%", background: "#111" }} />
      </div>
    </Card>
  );
}

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [msg, setMsg] = useState("loading...");
  const [lastUpdated, setLastUpdated] = useState(null);

  async function fetchLatest() {
    try {
      const res = await fetch("/api/dashboard/latest");
      if (res.status === 204) {
        setMsg("아직 집계 데이터가 없습니다 (Spark가 Redis에 쓰기 전)");
        return;
      }
      if (!res.ok) {
        setMsg(`API error: ${res.status}`);
        return;
      }
      const json = await res.json();
      setData(json);
      setMsg("");
      setLastUpdated(new Date());
    } catch (e) {
      setMsg(`fetch failed: ${String(e)}`);
    }
  }

  useEffect(() => {
    fetchLatest();
    const id = setInterval(fetchLatest, 5000);
    return () => clearInterval(id);
  }, []);

  const totalSeries = useMemo(() => {
    if (!data?.total) return [];
    return data.total.map(p => ({
      t: p.t,
      label: epochToLabel(p.t),
      v: p.v
    }));
  }, [data]);

  // byType → stacked area용으로 (t 기준 row merge)
  const typeStack = useMemo(() => {
    if (!data?.byType) return [];
    const map = new Map();
    Object.entries(data.byType).forEach(([type, arr]) => {
      (arr || []).forEach(p => {
        const row = map.get(p.t) || { t: p.t, label: epochToLabel(p.t) };
        row[type] = p.v;
        map.set(p.t, row);
      });
    });
    const rows = Array.from(map.values()).sort((a, b) => a.t - b.t);

    // 누락 타입 0으로 채우기 (차트 안정)
    const keys = Object.keys(data.byType);
    for (const r of rows) {
      for (const k of keys) {
        if (r[k] === undefined) r[k] = 0;
      }
    }
    return rows;
  }, [data]);

  const typeKeys = useMemo(() => (data?.byType ? Object.keys(data.byType) : []), [data]);

  const topWiki = useMemo(() => {
    if (!data?.topWiki) return [];
    return data.topWiki.map(x => ({ k: x.k, v: x.v }));
  }, [data]);

  return (
    <div style={{
      maxWidth: 1200,
      margin: "0 auto",
      padding: 24,
      fontFamily: "system-ui",
      background: "#fafafa",
      minHeight: "100vh"
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
        <div>
          <h2 style={{ margin: 0 }}>Wikimedia Realtime Dashboard</h2>
          <div style={{ color: "#666", marginTop: 4, fontSize: 13 }}>
            5초마다 최신 집계 데이터를 갱신합니다.
          </div>
        </div>
        <div style={{ color: "#666", fontSize: 13 }}>
          {lastUpdated ? `updated ${lastUpdated.toLocaleTimeString()}` : ""}
        </div>
      </div>

      {msg && (
        <div style={{
          marginTop: 14,
          padding: 12,
          borderRadius: 12,
          background: "#fff3cd",
          border: "1px solid #ffeeba",
          color: "#856404"
        }}>
          {msg}
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 16, marginTop: 16 }}>
        <Card title="전체 이벤트량 (10초 버킷)">
          <div style={{ width: "100%", height: 280 }}>
            <ResponsiveContainer>
              <LineChart data={totalSeries}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="v" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Card>

        <Gauge ratio={data?.bot?.ratio ?? 0} total={data?.bot?.total ?? 0} bot={data?.bot?.bot ?? 0} />
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 16, marginTop: 16 }}>
        <Card title="이벤트 타입별 분포 (스택)">
          <div style={{ width: "100%", height: 330 }}>
            <ResponsiveContainer>
              <AreaChart data={typeStack}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" />
                <YAxis />
                <Tooltip />
                <Legend />
                {typeKeys.map(k => (
                  <Area key={k} type="monotone" dataKey={k} stackId="1" dot={false} />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Card>

        <Card title="Top Wiki (최근 5분)">
          <div style={{ width: "100%", height: 330 }}>
            <ResponsiveContainer>
              <BarChart data={topWiki} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" />
                <YAxis dataKey="k" type="category" width={140} />
                <Tooltip />
                <Bar dataKey="v" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>
      </div>
    </div>
  );
}
