import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip } from "recharts";
import Card from "./Card";

export default function TotalEventsChart({ data, bucketSec = 10 }) {
  return (
    // bucketSec: 서버가 몇 초 버킷으로 집계했는지(타이틀에 표시)
    <Card title={`전체 이벤트량 (${bucketSec}초 버킷)`}>
      <div style={{ width: "100%", height: 280 }}>
        <ResponsiveContainer>
          <LineChart data={data}>
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
            <Line
              type="monotone"
              dataKey="v"
              stroke="var(--color-primary)"
              strokeWidth={2}
              dot={false}
              name="이벤트 수"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
