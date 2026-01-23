# Crypto Stream Engine

## Description:

Crypto Stream Engine is a high-performance market data aggregation and analysis engine that collects real-time trading data from multiple cryptocurrency exchanges, processes it, and generates alerts based on configurable rules.

## Overall project structure:

cmd/server/main.go: Entry point for the application
internal/config: Configuration management
internal/domain: Domain models and interfaces
internal/provider: Exchange connectors (Binance, Bybit, etc.)
internal/pipeline: Data processing pipeline
internal/processor: Alert processors
internal/metrics: Metrics collection
internal/web: HTTP server and WebSocket endpoints

## Abstraction in domain package:

Provider interface: Defines the contract for exchange connectors
Tick struct: Represents a single market data point
Alert struct: Represents a calculated anomaly
AggregatedData struct: Represents processed market data

## TODO:

- Multi-exchange support: Collects data from multiple exchanges via WebSocket
- Real-time processing: Processes market data in real-time using a streaming pipeline
- Alerting system: Generates alerts based on configurable rules
- Metrics collection: Collects and exposes metrics for monitoring
- Extensible architecture: Easy to add new exchanges and alert processors

