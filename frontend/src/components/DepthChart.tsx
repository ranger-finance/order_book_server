import { useMemo, memo } from "react";
import {
  ComposedChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import type { OrderbookUpdate } from "../types";
import { normalizePrice, normalizeSize } from "../utils/precision";

interface DepthChartProps {
  data: OrderbookUpdate;
}

interface DepthDataPoint {
  price: number;
  bidDepth: number | null;
  askDepth: number | null;
}

function DepthChartInner({ data }: DepthChartProps) {
  const chartData = useMemo(() => {
    const { oracle_curve } = data.analysis;
    const midPrice = normalizePrice(oracle_curve.reference_price, data.orderbook.exchange);

    const points: DepthDataPoint[] = [];

    // Add bid curve points (in reverse order for proper visualization)
    [...oracle_curve.bid_curve].reverse().forEach((point) => {
      points.push({
        price: normalizePrice(point.price, data.orderbook.exchange),
        bidDepth: normalizeSize(point.cumulative_depth, data.orderbook.exchange),
        askDepth: null,
      });
    });

    // Add ask curve points
    oracle_curve.ask_curve.forEach((point) => {
      points.push({
        price: normalizePrice(point.price, data.orderbook.exchange),
        bidDepth: null,
        askDepth: normalizeSize(point.cumulative_depth, data.orderbook.exchange),
      });
    });

    // Sort by price
    points.sort((a, b) => a.price - b.price);

    return { points, midPrice };
  }, [data]);

  const { stats } = data.analysis;

  return (
    <div className="chart-container">
      <div className="chart-header">
        <h3>Depth Chart - {data.orderbook.symbol}</h3>
        <span className="exchange-badge">{data.orderbook.exchange}</span>
      </div>

      <div className="stats-row">
        <div className="stat">
          <span className="label">Best Bid</span>
          <span className="value bid">
            {stats.best_bid ? normalizePrice(stats.best_bid, data.orderbook.exchange).toFixed(2) : "-"}
          </span>
        </div>
        <div className="stat">
          <span className="label">Spread</span>
          <span className="value">
            {stats.spread_bps ? `${parseFloat(stats.spread_bps).toFixed(2)} bps` : "-"}
          </span>
        </div>
        <div className="stat">
          <span className="label">Best Ask</span>
          <span className="value ask">
            {stats.best_ask ? normalizePrice(stats.best_ask, data.orderbook.exchange).toFixed(2) : "-"}
          </span>
        </div>
      </div>

      <ResponsiveContainer width="100%" height={300}>
        <ComposedChart data={chartData.points} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis
            dataKey="price"
            type="number"
            domain={["dataMin", "dataMax"]}
            tickFormatter={(value) => value.toFixed(2)}
            stroke="#888"
          />
          <YAxis stroke="#888" />
          <Tooltip
            contentStyle={{ backgroundColor: "#1a1a2e", border: "1px solid #333" }}
            formatter={(value) => (typeof value === "number" ? value.toFixed(4) : value)}
            labelFormatter={(label) => `Price: ${parseFloat(String(label)).toFixed(2)}`}
          />
          <ReferenceLine
            x={chartData.midPrice}
            stroke="#fff"
            strokeDasharray="5 5"
            label={{ value: "Mid", fill: "#fff", position: "top" }}
          />
          <Area
            type="stepAfter"
            dataKey="bidDepth"
            stroke="#00c853"
            fill="#00c853"
            fillOpacity={0.3}
            name="Bid Depth"
            connectNulls={false}
            isAnimationActive={false}
          />
          <Area
            type="stepAfter"
            dataKey="askDepth"
            stroke="#ff5252"
            fill="#ff5252"
            fillOpacity={0.3}
            name="Ask Depth"
            connectNulls={false}
            isAnimationActive={false}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

export const DepthChart = memo(DepthChartInner);
