import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from "recharts";
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

export default function EventsByTypeChart({ data, typeKeys }) {
  return (
    <Card title="이벤트 타입별 분포 (스택)">
      <div style={{ width: "100%", height: 330 }}>
        <ResponsiveContainer>
          <AreaChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 12 }}
              stroke="#757575"
            />
            <YAxis
              tick={{ fontSize: 12 }}
              stroke="#757575"
            />
            <Tooltip
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
            {typeKeys.map((key, idx) => (
              <Area
                key={key}
                type="monotone"
                dataKey={key}
                stackId="1"
                stroke={COLORS[idx % COLORS.length]}
                fill={COLORS[idx % COLORS.length]}
                fillOpacity={0.6}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
