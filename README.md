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

## Infrastructure Details

- **Kafka Broker:** localhost:9092
- **Schema Registry:** http://localhost:8081
- **Zookeeper:** localhost:2181

## Development Roadmap

- [x] STEP 1: Infrastructure Setup (docker-compose.yml)
- [ ] STEP 2: Schema & Configuration (order.avsc, config.py)
- [ ] STEP 3: The Producer (producer.py)
- [ ] STEP 4: Basic Consumer & Aggregation (consumer.py)
- [ ] STEP 5: Error Simulation Wrapper
- [ ] STEP 6: Retry Logic
- [ ] STEP 7: Dead Letter Queue (DLQ)

## ## VS Code

- Select the interpreter at `./.venv/bin/python` via `Python: Select Interpreter`.

---

**Project Status:** STEP 1 Complete ✅
