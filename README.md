# Bigdata

**Kafka Producer-Consumer System with Fault Tolerance & DLQ**

This project implements a fault-tolerant streaming system for processing Order messages using Kafka, Avro serialization, retry logic, and Dead Letter Queue patterns.

## Architecture

- **Tech Stack:** Python 3.14, confluent-kafka, Avro, Docker Compose
- **Infrastructure:** Kafka Broker, Zookeeper, Schema Registry
- **Pattern:** Producer → Kafka Topic → Consumer (with running average calculation, retries, DLQ)

## Quick Start

### 1. Infrastructure Setup

### 1. Infrastructure Setup

Start the Confluent Platform (Zookeeper, Kafka, Schema Registry):

```zsh
docker compose up -d
```

Verify all containers are running:

```zsh
docker compose ps
```

Check logs if needed:

```zsh
docker compose logs -f kafka
docker compose logs -f schema-registry
```

Stop the infrastructure:

```zsh
docker compose down
```

### 2. Python Environment

1. Activate the virtual environment (zsh):

```zsh
cd /Users/ravishan/Downloads/Bigdata
source .venv/bin/activate
```

2. Verify Python and pip:

```zsh
python -V
pip -V
```

3. (Optional) Upgrade pip and install dependencies from `requirements.txt` if present:

```zsh
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Run the Producer

**Important:** Always activate the virtual environment first!

```zsh
source .venv/bin/activate
python producer.py
```

Stop with `Ctrl+C`.

### 4. Run the Consumer

**In a separate terminal**, activate venv and run:

```zsh
source .venv/bin/activate
python consumer.py
```

The consumer will:
- Read messages from the `orders` topic
- Calculate running average of prices
- Display real-time statistics

Stop with `Ctrl+C`.

### 5. Monitor the Dead Letter Queue (Optional)

**In a third terminal**, monitor failed messages:

```zsh
source .venv/bin/activate
python dlq_monitor.py
```

This will display all messages that failed processing and were sent to the DLQ.

## Infrastructure Details

- **Kafka Broker:** localhost:9092
- **Schema Registry:** http://localhost:8081
- **Zookeeper:** localhost:2181

## Development Roadmap

- [x] STEP 1: Infrastructure Setup (docker-compose.yml)
- [x] STEP 2: Schema & Configuration (order.avsc, config.py)
- [x] STEP 3: The Producer (producer.py)
- [x] STEP 4: Basic Consumer & Aggregation (consumer.py)
- [x] STEP 5: Error Simulation Wrapper
- [x] STEP 6: Retry Logic
- [x] STEP 7: Dead Letter Queue (DLQ)

## ## VS Code

- Select the interpreter at `./.venv/bin/python` via `Python: Select Interpreter`.

---

**Project Status:** ✅ ALL STEPS COMPLETE - Production Ready!

### Files Structure
```
Bigdata/
├── docker-compose.yml    # Kafka infrastructure
├── order.avsc            # Avro schema definition
├── config.py             # Shared configuration
├── producer.py           # Avro producer (generates orders)
├── consumer.py           # Avro consumer (running average + DLQ)
├── dlq_monitor.py        # DLQ monitoring tool
├── requirements.txt      # Python dependencies
├── .venv/                # Python virtual environment
└── README.md
```
