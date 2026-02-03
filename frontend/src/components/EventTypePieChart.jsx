import { ResponsiveContainer, PieChart, Pie, Cell, Tooltip, Legend } from "recharts";
import Card from "./Card";

const COLORS = [
  "#1976d2",
  "#dc004e",
  "#4caf50",
  "#ff9800",
  "#9c27b0",
  "#00bcd4",
  "#795548",
  "#607d8b"
];

export default function EventTypePieChart({ data }) {
  return (
    <Card title="이벤트 타입별 분포 (비율)">
      <div style={{ width: "100%", height: 330 }}>
        {data && data.length > 0 ? (
          <ResponsiveContainer>
            <PieChart>
              <Pie
                data={data}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                outerRadius={100}
                label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
                labelLine={true}
              >
                {data.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                formatter={(value, name) => [value.toLocaleString(), name]}
                contentStyle={{
                  backgroundColor: "white",
                  border: "1px solid #e0e0e0",
                  borderRadius: 8,
                  fontSize: 12
                }}
              />
              <Legend
                wrapperStyle={{ fontSize: 12 }}
              />
            </PieChart>
          </ResponsiveContainer>
        ) : (
          <div style={{
            display: "flex",
            justifyContent: "center",
            alignItems: "center",
            height: "100%",
            color: "var(--color-text-secondary)"
          }}>
            데이터가 없습니다
          </div>
        )}
      </div>
    </Card>
  );
}
