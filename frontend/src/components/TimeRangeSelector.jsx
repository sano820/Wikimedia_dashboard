// 시간 범위 선택 UI

export default function TimeRangeSelector({ value, onChange, options }) {
  return (
    <div style={{
      display: "flex",
      alignItems: "center",
      gap: 8
    }}>
      <label style={{
        fontSize: 14,
        color: "var(--color-text-secondary)",
        fontWeight: 500
      }}>
        시간 범위:
      </label>
      <select
        value={value}
        onChange={(e) => onChange(parseInt(e.target.value, 10))}
        style={{
          padding: "6px 12px",
          borderRadius: 6,
          border: "1px solid var(--color-border)",
          fontSize: 14,
          backgroundColor: "white",
          cursor: "pointer",
          outline: "none"
        }}
      >
        {options.map(option => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </div>
  );
}
