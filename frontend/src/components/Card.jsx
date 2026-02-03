export default function Card({ title, children, className = "" }) {
  return (
    <div className={`card ${className}`} style={{
      border: "1px solid var(--color-border)",
      borderRadius: 12,
      padding: 20,
      background: "var(--color-surface)",
      boxShadow: "var(--shadow-sm)"
    }}>
      {title && (
        <h3 style={{
          fontSize: 16,
          fontWeight: 700,
          marginBottom: 16,
          color: "var(--color-text-primary)"
        }}>
          {title}
        </h3>
      )}
      {children}
    </div>
  );
}
