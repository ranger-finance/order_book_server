import { useMemo, memo } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  Cell,
} from "recharts";
import type { OrderbookUpdate } from "../types";
import { normalizePrice, normalizeSize } from "../utils/precision";

interface LiquidityHistogramProps {
  data: OrderbookUpdate;
}

interface HistogramDataPoint {
  priceRange: string;
  price: number;
  liquidity: number;
  isBid: boolean;
}

function LiquidityHistogramInner({ data }: LiquidityHistogramProps) {
  const chartData = useMemo(() => {
    const { liquidity_distribution, oracle_curve } = data.analysis;
    const midPrice = normalizePrice(oracle_curve.reference_price, data.orderbook.exchange);

    const points: HistogramDataPoint[] = liquidity_distribution.map((bucket) => {
      const priceLow = normalizePrice(bucket.price_low, data.orderbook.exchange);
      const priceHigh = normalizePrice(bucket.price_high, data.orderbook.exchange);
      const midBucket = (priceLow + priceHigh) / 2;

      return {
        priceRange: `${priceLow.toFixed(2)}-${priceHigh.toFixed(2)}`,
        price: midBucket,
        liquidity: normalizeSize(bucket.liquidity, data.orderbook.exchange),
        isBid: bucket.is_bid,
      };
    });

    // Sort by price
    points.sort((a, b) => a.price - b.price);

    return { points, midPrice };
  }, [data]);

  const { stats } = data.analysis;

  return (
    <div className="chart-container">
      <div className="chart-header">
        <h3>Liquidity Distribution - {data.orderbook.symbol}</h3>
        <span className="exchange-badge">{data.orderbook.exchange}</span>
      </div>

      <div className="stats-row">
        <div className="stat">
          <span className="label">Bid Liquidity</span>
          <span className="value bid">{normalizeSize(stats.total_bid_liquidity, data.orderbook.exchange).toFixed(2)}</span>
        </div>
        <div className="stat">
          <span className="label">Imbalance</span>
          <span className="value">
            {stats.imbalance_ratio ? `${parseFloat(stats.imbalance_ratio).toFixed(2)}x` : "-"}
          </span>
        </div>
        <div className="stat">
          <span className="label">Ask Liquidity</span>
          <span className="value ask">{normalizeSize(stats.total_ask_liquidity, data.orderbook.exchange).toFixed(2)}</span>
        </div>
      </div>

      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData.points} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis dataKey="price" tickFormatter={(value) => value.toFixed(2)} stroke="#888" />
          <YAxis stroke="#888" />
          <Tooltip
            contentStyle={{ backgroundColor: "#1a1a2e", border: "1px solid #333" }}
            formatter={(value) => (typeof value === "number" ? value.toFixed(4) : value)}
            labelFormatter={(label) => `Price: ${parseFloat(String(label)).toFixed(2)}`}
          />
          <ReferenceLine x={chartData.midPrice} stroke="#fff" strokeDasharray="5 5" />
          <Bar dataKey="liquidity" name="Liquidity" isAnimationActive={false}>
            {chartData.points.map((entry, index) => (
              <Cell
                key={`cell-${index}`}
                fill={entry.isBid ? "#00c853" : "#ff5252"}
                fillOpacity={0.7}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

export const LiquidityHistogram = memo(LiquidityHistogramInner);
