import Card from "./Card";

export default function BotRatioGauge({ ratio, total, bot, windowSec = 60 }) {
  const percentage = Math.round((ratio ?? 0) * 100);

  return (
    <Card title={`Bot 비율 (최근 ${windowSec}초)`}>
      <div style={{
        display: "flex",
        alignItems: "baseline",
        gap: 16,
        marginBottom: 20
      }}>
        <div style={{
          fontSize: 56,
          fontWeight: 900,
          color: "var(--color-primary)",
          lineHeight: 1
        }}>
          {percentage}%
        </div>
        <div style={{
          color: "var(--color-text-secondary)",
          fontSize: 14
        }}>
          <div>Bot: {(bot ?? 0).toLocaleString()}</div>
          <div>Total: {(total ?? 0).toLocaleString()}</div>
        </div>
      </div>

      <div style={{
        height: 12,
        background: "#e0e0e0",
        borderRadius: 999,
        overflow: "hidden"
      }}>
        <div style={{
          width: `${percentage}%`,
          height: "100%",
          background: "linear-gradient(90deg, var(--color-primary) 0%, var(--color-secondary) 100%)",
          transition: "width 0.3s ease"
        }} />
      </div>
    </Card>
  );
}
