// src/components/ContentChangeGauge.jsx
import Card from "./Card";

export default function ContentChangeGauge({
  ratio,                    // 0~1
  changedCount,
  totalRevisionCount,
  windowSec = 60            // 표시용
}) {
  const percentage = Math.round((ratio ?? 0) * 100);

  return (
    <Card title={`Content 변경 비율 (최근 ${windowSec}초)`}>
      <div style={{ display: "flex", alignItems: "baseline", gap: 16, marginBottom: 20 }}>
        <div style={{
          fontSize: 56,
          fontWeight: 900,
          color: "var(--color-primary)",
          lineHeight: 1
        }}>
          {percentage}%
        </div>

        <div style={{ color: "var(--color-text-secondary)", fontSize: 14 }}>
          <div>Changed: {(changedCount ?? 0).toLocaleString()}</div>
          <div>Total revisions: {(totalRevisionCount ?? 0).toLocaleString()}</div>
        </div>
      </div>

      <div style={{
        height: 12,
        borderRadius: 999,
        background: "#e9eef5",
        overflow: "hidden"
      }}>
        <div style={{
          width: `${percentage}%`,
          height: "100%",
          background: "var(--color-primary)"
        }} />
      </div>

      <div style={{ marginTop: 10, fontSize: 12, color: "var(--color-text-secondary)" }}>
      </div>
    </Card>

  );
}
