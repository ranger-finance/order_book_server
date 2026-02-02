import { useMemo } from "react";
import { motion } from "motion/react";
import type { OrderbookUpdate } from "../types";
import { formatPrice, formatSize, normalizeSize } from "../utils/precision";

interface OrderbookTableProps {
  data: OrderbookUpdate;
  maxLevels?: number;
}

export function OrderbookTable({ data, maxLevels = 20 }: OrderbookTableProps) {
  const { bids, asks, maxTotal } = useMemo(() => {
    const bids = data.orderbook.bids.slice(0, maxLevels);
    const asks = data.orderbook.asks.slice(0, maxLevels);

    // Calculate total volume for depth visualization
    const totalBidSize = bids.reduce(
      (acc, level) => acc + normalizeSize(level.size, data.orderbook.exchange),
      0,
    );
    const totalAskSize = asks.reduce(
      (acc, level) => acc + normalizeSize(level.size, data.orderbook.exchange),
      0,
    );
    const maxTotal = Math.max(totalBidSize, totalAskSize);

    return { bids, asks, maxTotal };
  }, [data, maxLevels]);

  const getDepthWidth = (size: string) => {
    const numericSize = normalizeSize(size, data.orderbook.exchange);
    const percentage = maxTotal > 0 ? (numericSize / maxTotal) * 100 : 0;
    // Multiply by 5 to make bars more visible, cap at 100%
    return `${Math.min(percentage * 5, 100)}%`;
  };

  const { stats } = data.analysis;

  return (
    <div className="orderbook-table">
      <div className="chart-header">
        <h3>Orderbook - {data.orderbook.symbol}</h3>
        <span className="exchange-badge">{data.orderbook.exchange}</span>
      </div>

      <div className="table-container">
        <div className="table-side bids-side">
          <div className="table-header">
            <span>Size</span>
            <span>Bid Price</span>
          </div>
          {bids.map((level, i) => (
            <div key={i} className="table-row bid">
              <motion.div
                className="depth-bar"
                initial={{ width: 0 }}
                animate={{ width: getDepthWidth(level.size) }}
                transition={{ type: "spring", stiffness: 100, damping: 20 }}
                style={{
                  backgroundColor: "rgba(0, 200, 83, 0.15)",
                }}
              />
              <span className="size">
                {formatSize(level.size, data.orderbook.exchange)}
              </span>
              <span className="price">
                {formatPrice(level.price, data.orderbook.exchange)}
              </span>
            </div>
          ))}
        </div>

        <div className="spread-display">
          <span className="spread-label">Spread</span>
          <span className="spread-value">
            {stats.spread
              ? formatPrice(stats.spread, data.orderbook.exchange)
              : "-"}
          </span>
          <span className="spread-bps">
            {stats.spread_bps
              ? `(${parseFloat(stats.spread_bps).toFixed(2)} bps)`
              : ""}
          </span>
        </div>

        <div className="table-side asks-side">
          <div className="table-header">
            <span>Ask Price</span>
            <span>Size</span>
          </div>
          {asks.map((level, i) => (
            <div key={i} className="table-row ask">
              <motion.div
                className="depth-bar"
                initial={{ width: 0 }}
                animate={{ width: getDepthWidth(level.size) }}
                transition={{ type: "spring", stiffness: 50, damping: 10 }}
                style={{
                  backgroundColor: "rgba(255, 82, 82, 0.15)",
                }}
              />
              <span className="price">
                {formatPrice(level.price, data.orderbook.exchange)}
              </span>
              <span className="size">
                {formatSize(level.size, data.orderbook.exchange)}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
