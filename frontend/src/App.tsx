import { useState } from "react";
import { useWebSocket } from "./hooks/useWebSocket";
import { OrderbookTable } from "./components/OrderbookTable";
import type { Exchange, Token } from "./types";
import { AVAILABLE_TOKENS } from "./types";
import "./App.css";
import { DepthChart } from "./components/DepthChart";
import { LiquidityHistogram } from "./components/LiquidityHistogram";

function App() {
  const [selectedExchange, setSelectedExchange] = useState<Exchange | "all">(
    "all",
  );
  const [selectedToken, setSelectedToken] = useState<Token | "all">("all");
  const { orderbooks, isConnected, error } = useWebSocket(
    selectedExchange,
    selectedToken,
  );

  // Backend handles filtering, just convert to array
  const orderbookList = Array.from(orderbooks.values());

  return (
    <div className="app">
      <header className="header">
        <h1>Drift + Hyperliquid Orderbook Streamer</h1>
        <div className="controls">
          <select
            value={selectedExchange}
            onChange={(e) =>
              setSelectedExchange(e.target.value as Exchange | "all")
            }
            className="exchange-selector"
          >
            <option value="all">All Exchanges</option>
            <option value="hyperliquid">Hyperliquid</option>
            <option value="drift">Drift</option>
          </select>

          <select
            value={selectedToken}
            onChange={(e) => setSelectedToken(e.target.value as Token | "all")}
            className="token-selector"
          >
            <option value="all">All Tokens</option>
            {AVAILABLE_TOKENS.map((token) => (
              <option key={token} value={token}>
                {token}
              </option>
            ))}
          </select>

          <div className="connection-status">
            <span
              className={`status-indicator ${isConnected ? "connected" : "disconnected"}`}
            />
            <span>{isConnected ? "Connected" : "Disconnected"}</span>
          </div>
        </div>
      </header>

      {error && (
        <div className="error-banner">{error} - Attempting to reconnect...</div>
      )}

      {orderbookList.length === 0 ? (
        <div className="loading">
          <p>Waiting for orderbook data...</p>
          <p className="hint">
            Make sure the backend server is running on localhost:8080
          </p>
        </div>
      ) : (
        <div className="dashboard">
          {orderbookList.map((update) => {
            if (
              selectedExchange !== "all" &&
              update.orderbook.exchange !== selectedExchange
            ) {
              return null;
            }
            if (
              selectedToken !== "all" &&
                update.orderbook.symbol !== selectedToken
            ) {
              return null;
            }
            const key = `${update.orderbook.exchange}:${update.orderbook.symbol}`;
            return (
              <div key={key} className="orderbook-panel">
                <div className="charts-row">
                  <DepthChart data={update} />
                  <LiquidityHistogram data={update} />
                </div>
                <OrderbookTable data={update} />
              </div>
            );
          })}
        </div>
      )}

      <footer className="footer">
        <p>POC - Real-time orderbook streaming with analysis overlay</p>
      </footer>
    </div>
  );
}

export default App;
