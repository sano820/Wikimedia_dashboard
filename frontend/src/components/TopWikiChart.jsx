import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip } from "recharts";
import Card from "./Card";

export default function TopWikiChart({ data, windowMin = 5 }) {
  return (
    <Card title={`Top Wiki (최근 ${windowMin}분)`}>
      <div style={{ width: "100%", height: 330 }}>
        <ResponsiveContainer>
          <BarChart data={data} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
            <XAxis
              type="number"
              tick={{ fontSize: 12 }}
              stroke="#757575"
            />
            <YAxis
              dataKey="k"
              type="category"
              width={140}
              tick={{ fontSize: 11 }}
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
            <Bar
              dataKey="v"
              fill="var(--color-success)"
              name="이벤트 수"
            />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
