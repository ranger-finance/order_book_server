import { useEffect, useRef, useState, useCallback } from "react";
import type { OrderbookUpdate, Exchange, Token } from "../types";

const WS_URL = "ws://localhost:8082";

interface ServerMessage {
  type: "snapshot" | "subscribed" | "unsubscribed" | "error";
  data?: OrderbookUpdate;
  exchange?: Exchange;
  symbol?: string;
  message?: string;
}

interface ClientMessage {
  action: "subscribe" | "unsubscribe";
  exchange: Exchange;
  symbol: string;
}

interface UseWebSocketReturn {
  orderbooks: Map<string, OrderbookUpdate>;
  isConnected: boolean;
  error: string | null;
  subscribe: (exchange: Exchange, symbol: string) => void;
  unsubscribe: (exchange: Exchange, symbol: string) => void;
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

  const sendMessage = useCallback((message: ClientMessage) => {
    const ws = wsRef.current;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }, []);

  const subscribe = useCallback(
    (exchangeParam: Exchange, symbol: string) => {
      const message: ClientMessage = {
        action: "subscribe",
        exchange: exchangeParam,
        symbol,
      };
      sendMessage(message);
    },
    [sendMessage],
  );

  const unsubscribe = useCallback(
    (exchangeParam: Exchange, symbol: string) => {
      const message: ClientMessage = {
        action: "unsubscribe",
        exchange: exchangeParam,
        symbol,
      };
      sendMessage(message);
    },
    [sendMessage],
  );

  useEffect(() => {
    mountedRef.current = true;

    const connect = () => {
      try {
        console.log("Connecting to", WS_URL);
        const ws = new WebSocket(WS_URL);
        wsRef.current = ws;

        ws.onopen = () => {
          if (!mountedRef.current) return;
          console.log("WebSocket connected");
          setIsConnected(true);
          setError(null);
          setOrderbooks(new Map());

          const currentExchange = exchangeRef.current;
          const currentToken = tokenRef.current;
          if (currentExchange !== "all" && currentToken !== "all") {
            let symbol = currentToken;
            if (currentExchange === "drift") {
              symbol += "-PERP";
            }
            subscribe(currentExchange, symbol);
          }
        };

        ws.onmessage = (event) => {
          if (!mountedRef.current) return;
          try {
            const message: ServerMessage = JSON.parse(event.data);
            switch (message.type) {
              case "snapshot":
                if (message.data) {
                  const key = `${message.data.orderbook.exchange}:${message.data.orderbook.symbol}`;
                  setOrderbooks((prev) => {
                    const newMap = new Map(prev);
                    newMap.set(key, message.data!);
                    return newMap;
                  });
                }
                break;
              case "subscribed":
                console.log("Subscribed to", message.exchange, message.symbol);
                break;
              case "unsubscribed":
                console.log("Unsubscribed from", message.exchange, message.symbol);
                break;
              case "error":
                setError(message.message || "Unknown error");
                break;
            }
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
  }, [subscribe, exchange, token]);

  return { orderbooks, isConnected, error, subscribe, unsubscribe };
}
