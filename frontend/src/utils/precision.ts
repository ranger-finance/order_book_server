import type { Exchange } from "../types";

export const DRIFT_PRICE_PRECISION = 1_000_000;
export const DRIFT_SIZE_PRECISION = 1_000_000_000;

export function normalizePrice(value: string, exchange: Exchange): number {
  const numValue = parseFloat(value);
  return exchange === "drift" ? numValue / DRIFT_PRICE_PRECISION : numValue;
}

export function normalizeSize(value: string, exchange: Exchange): number {
  const numValue = parseFloat(value);
  return exchange === "drift" ? numValue / DRIFT_SIZE_PRECISION : numValue;
}

export function formatPrice(value: string, exchange: Exchange): string {
  return normalizePrice(value, exchange).toFixed(2);
}

export function formatSize(value: string, exchange: Exchange): string {
  return normalizeSize(value, exchange).toFixed(4);
}
