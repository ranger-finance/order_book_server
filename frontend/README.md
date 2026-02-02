# Orderbook Visualizer Frontend

A real-time orderbook visualization dashboard for Drift and Hyperliquid exchanges.

## Tech Stack

- **Framework:** [React 19](https://react.dev/)
- **Build Tool:** [Vite](https://vitejs.dev/)
- **Language:** [TypeScript](https://www.typescriptlang.org/)
- **Package Manager:** [Bun](https://bun.sh/)
- **Animations:** [Motion](https://motion.dev/) (formerly Framer Motion)
- **Charts:** [Recharts](https://recharts.org/)
- **Communication:** WebSockets

## Getting Started

### Prerequisites

You need [Bun](https://bun.sh/) installed on your machine.

### Installation

```bash
bun install
```

### Development

Start the development server:

```bash
bun dev
```

### Build

Build for production:

```bash
bun run build
```

## Features

- **Real-time Orderbook:** Live bids and asks from Drift and Hyperliquid.
- **Depth Visualization:** Overlay bars in the orderbook table showing relative liquidity.
- **Filtering:** Filter by exchange and token (SOL, BTC, ETH).
