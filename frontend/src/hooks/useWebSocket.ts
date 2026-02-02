import { useEffect, useRef, useState } from "react";
import type { OrderbookUpdate, Exchange, Token } from "../types";

const WS_URL = "ws://localhost:8080/ws";

interface UseWebSocketReturn {
  orderbooks: Map<string, OrderbookUpdate>;
  isConnected: boolean;
  error: string | null;
}

export function useWebSocket(
  exchange: Exchange | "all" = "all",
  token: Token | "all" = "all",
): UseWebSocketReturn {
  const [orderbooks, setOrderbooks] = useState<Map<string, OrderbookUpdate>>(new Map());
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);
  const exchangeRef = useRef(exchange);
  const tokenRef = useRef(token);

  useEffect(() => {
    exchangeRef.current = exchange;
  }, [exchange]);

  useEffect(() => {
    tokenRef.current = token;
  }, [token]);

  useEffect(() => {
    mountedRef.current = true;

    const connect = () => {
      const currentExchange = exchangeRef.current;
      const currentToken = tokenRef.current;

      try {
        const url = `${WS_URL}?exchange=${currentExchange}&token=${currentToken}`;
        console.log("Connecting to", url);
        const ws = new WebSocket(url);
        wsRef.current = ws;

        ws.onopen = () => {
          if (!mountedRef.current) return;
          console.log("WebSocket connected");
          setIsConnected(true);
          setError(null);
          setOrderbooks(new Map());
        };

        ws.onmessage = (event) => {
          if (!mountedRef.current) return;
          try {
            const update: OrderbookUpdate = JSON.parse(event.data);
            const key = `${update.orderbook.exchange}:${update.orderbook.symbol}`;
            setOrderbooks((prev) => {
              const newMap = new Map(prev);
              newMap.set(key, update);
              return newMap;
            });
          } catch (e) {
            console.error("Failed to parse message:", e);
          }
        };

        ws.onclose = () => {
          if (!mountedRef.current) return;
          console.log("WebSocket disconnected");
          setIsConnected(false);
          reconnectTimeoutRef.current = setTimeout(connect, 3000);
        };

        ws.onerror = (e) => {
          if (!mountedRef.current) return;
          console.error("WebSocket error:", e);
          setError("Connection error");
        };
      } catch (e) {
        console.error("Failed to connect:", e);
        if (!mountedRef.current) return;
        setError("Failed to connect");
        reconnectTimeoutRef.current = setTimeout(connect, 3000);
      }
    };

    connect();

    return () => {
      mountedRef.current = false;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [exchange, token]);

  return { orderbooks, isConnected, error };
}
