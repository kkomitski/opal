# OPAL: Order Processing and Limit Matching Engine

## Overview
OPAL is a high-performance, modular trading system for simulating, stress-testing, and running electronic order books. It is designed for research, benchmarking, and development of matching engines, trading clients, and market microstructure analysis.

## Modules

- **matching-engine/**: Core order book and matching engine. Handles order matching, price discovery, and market data. Uses LMAX Disruptor, Netty, Agrona, and FastUtil for speed and concurrency.
- **client/**: Load testing client for simulating thousands of connections and orders per second. Highly configurable for spread, bias, volatility, and order flow realism.
- **utils/**: Shared utilities for market configuration, decoding, and support classes.
- **server/**: Spring Boot server for hosting market configuration and REST APIs.

## Features

- Limit order book with price/time priority
- Configurable price collar, tick size, and decimal precision
- Stress test client with Gaussian/exponential order flow, spread crossing, and market drift
- Binary order file generator for offline benchmarking
- Prometheus metrics integration
- JVM tuning for low-latency, high-throughput workloads

## Getting Started

### Prerequisites
- Java 17+
- Maven

### Build All Modules
```sh
mvn clean install
```

### Run Matching Engine
```sh
cd matching-engine
mvn package
java -jar target/matching-engine-*.jar
```

### Run Load Test Client
```sh
cd client
mvn package
java -jar target/client-*.jar [host] [port] [connections] [orders/sec] [duration]
```
Example:
```
java -jar target/client-*.jar 127.0.0.1 42069 250 10000 60
```

### Run Server (Market Config REST API)
```sh
cd server
mvn spring-boot:run
```

## Configuration

- Market configuration is loaded from XML or REST endpoint (see server module)
- Client order flow is controlled by constants in `LoadTestClient.java`:
	- `MAX_PRICE_DEVIATION`, `TARGET_SPREAD`, `PRICE_BIAS`, `CURVE_STEEPNESS_FACTOR`, `SPREAD_CROSS_PROBABILITY`, `OUTLIER_PROBABILITY`, `VOLATILITY_FACTOR`

## JVM Notes (Agrona/Aeron)

### `--add-opens` flags

Agrona/Aeron use low-level JDK internals for performance. On newer JDKs you may need these JVM args when running tests or applications:

- `--add-opens jdk.unsupported/sun.misc=ALL-UNNAMED`
- `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED`
- `--add-opens java.base/java.util.zip=ALL-UNNAMED`

### Endianness

Agrona `DirectBuffer` operations default to the platform endianness. If you have components running on different endianness, specify `ByteOrder` explicitly when reading/writing fields.

## Architecture

- Matching engine uses LMAX Disruptor for event processing
- Netty for TCP networking
- Agrona and FastUtil for efficient data structures
- Prometheus for metrics
- Spring Boot for REST API

## Example Use Cases

- Benchmarking matching engine throughput and latency
- Simulating realistic market order flow for research
- Stress testing server and network infrastructure
- Generating binary order files for offline replay

## License

MIT (see LICENSE)
