# Kafka Fault-Tolerant Producer-Consumer System

A production-ready Kafka streaming system demonstrating fault tolerance, retry logic with exponential backoff, and Dead Letter Queue (DLQ) pattern for order processing.

## 📋 Overview

This system processes order messages in real-time using:
- **Kafka** for distributed messaging
- **Avro** for schema-based serialization
- **Schema Registry** for schema management
- **Retry Logic** with exponential backoff for transient failures
- **Dead Letter Queue** for poison pills and exhausted retries
- **Running Average** calculation for real-time analytics

---

## 🎬 Live Demo

Watch the complete system demonstration:

<video width="100%" controls autoplay muted loop>
  <source src="./demo.mp4" type="video/mp4">
  Your browser does not support the video tag. <a href="./demo.mp4">Download the demo video</a>
</video>

[Download demo.mp4](./demo.mp4)

The video demonstrates:
- Starting Kafka infrastructure
- Producer generating synthetic orders
- Consumer processing with running average
- Retry logic for transient errors (exponential backoff)
- DLQ handling for failed messages
- End-to-end fault tolerance

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop installed and running
- Python 3.x with pip
- Git (optional)

### Step 1: Start Kafka Infrastructure

```bash
docker compose up -d
```

**Expected Output:**
```
[+] Running 3/3
 ✔ Container zookeeper        Started
 ✔ Container kafka            Started
 ✔ Container schema-registry  Started
```

**Verify containers are running:**
```bash
docker compose ps
```

All 3 containers should show status "Up".

### Step 2: Set Up Python Environment

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Run the Producer

**Terminal 1:**
```bash
source .venv/bin/activate
python producer.py
```

**Expected Output:**
```
🚀 Starting Kafka Avro Producer...
📡 Connected to: localhost:9092
📋 Schema Registry: http://localhost:8081
📨 Publishing to topic: orders
------------------------------------------------------------

📦 [Message #1] Producing order:
   OrderID: fe048d1e-fbac-4fcc-8ad7-a0b2c4513520
   Product: Webcam
   Price: $780.79
✅ Message delivered to orders [partition 0] at offset 0

📦 [Message #2] Producing order:
   OrderID: 874bf558-b400-4154-b7ac-bcc427224b23
   Product: Docking Station
   Price: $670.31
✅ Message delivered to orders [partition 0] at offset 1
```

The producer will:
- Generate random orders every 1 second
- Use random products (Laptop, Mouse, Keyboard, etc.)
- Prices range from $9.99 to $999.99
- Show delivery confirmation with offset

### Step 4: Run the Consumer

**Terminal 2 (keep producer running):**
```bash
source .venv/bin/activate
python consumer.py
```

**Expected Output (Success Case):**
```
🚀 Starting Kafka Avro Consumer...
📡 Connected to: localhost:9092
📋 Schema Registry: http://localhost:8081
📥 Consuming from topic: orders
👥 Consumer Group: order-processor-group
☠️ DLQ Topic: orders_dlq
------------------------------------------------------------

⏳ Waiting for messages... (Press Ctrl+C to stop)

📦 [Message #1] Consumed order:
   OrderID: fe048d1e-fbac-4fcc-8ad7-a0b2c4513520
   Product: Webcam
   Price: $780.79
   🔢 Running Average: $780.79 (from 1 orders)
   📍 Partition: 0 | Offset: 0

📦 [Message #2] Consumed order:
   OrderID: 874bf558-b400-4154-b7ac-bcc427224b23
   Product: Docking Station
   Price: $670.31
   🔢 Running Average: $725.55 (from 2 orders)
   📍 Partition: 0 | Offset: 1
```

**Error Handling - Retry with Exponential Backoff:**

When a `ConnectionError` occurs (~5% of messages), you'll see:
```
🔥 ERROR encountered:
   ⚠️ SIMULATED ERROR: Temporary service unavailable while processing order abc-123
   ⚠️ Retryable error (ConnectionError) - Attempt 1/3
   ⏳ Retrying in 1 seconds...
   
   [1 second pause]
   
   ⚠️ Retryable error (ConnectionError) - Attempt 2/3
   ⏳ Retrying in 2 seconds...
   
   [2 second pause]
   
   ⚠️ Retryable error (ConnectionError) - Attempt 3/3
   ⏳ Retrying in 4 seconds...
   
   [4 second pause]
   
   ❌ Max retries (3) exhausted

🔥 RETRY EXHAUSTED - ConnectionError:
   ⚠️ SIMULATED ERROR: Temporary service unavailable...
   OrderID: abc-123
   ☠️ Sent to DLQ: orders_dlq
   Reason: ConnectionError - Temporary service unavailable...
   ✅ Offset will be committed - moving to next message
```

**Poison Pill - No Retry:**

When a `ValueError` occurs (~5% of messages), you'll see:
```
🔥 ERROR encountered:
   ❌ SIMULATED ERROR: Invalid data for order xyz-789 - Price validation failed
   ❌ Non-retryable error (ValueError) - will not retry

🔥 POISON PILL - ValueError:
   ❌ SIMULATED ERROR: Invalid data for order xyz-789...
   OrderID: xyz-789
   ☠️ Sent to DLQ: orders_dlq
   Reason: ValueError - Invalid data...
   ✅ Offset will be committed - moving to next message
```

**Stop with `Ctrl+C` to see final statistics:**
```
⏹️  Consumer stopped by user

🔄 Closing consumer and DLQ producer...

============================================================
📊 FINAL STATISTICS
============================================================
Total Messages Received: 100
Successfully Processed: 88
Sent to DLQ: 12
Total Revenue: $42,156.78
Average Order Value: $479.05
============================================================

✅ Consumer shut down gracefully
```

### Step 5: Monitor Dead Letter Queue (Optional)

**Terminal 3 (keep producer and consumer running):**
```bash
source .venv/bin/activate
python dlq_monitor.py
```

**Expected Output:**
```
☠️ Starting Dead Letter Queue Monitor...
📡 Connected to: localhost:9092
📋 Schema Registry: http://localhost:8081
📥 Monitoring DLQ topic: orders_dlq
------------------------------------------------------------

⏳ Waiting for DLQ messages... (Press Ctrl+C to stop)

☠️ [DLQ Message #1]
   OrderID: abc-123
   Product: Keyboard
   Price: $89.99
   📍 Partition: 0 | Offset: 0
   ⏰ Timestamp: 1700518234000

☠️ [DLQ Message #2]
   OrderID: xyz-789
   Product: Monitor
   Price: $234.50
   📍 Partition: 0 | Offset: 1
```

The DLQ monitor shows all messages that failed processing after retries or were identified as poison pills.

---

## 🛑 Stopping the System

### Stop Applications
Press `Ctrl+C` in each terminal to stop:
- Producer
- Consumer
- DLQ Monitor

### Stop Kafka Infrastructure
```bash
docker compose down
```

**To completely clean up (removes all data):**
```bash
docker compose down -v
```

---

## ⚙️ Configuration

All settings are in `config.py`:

```python
BOOTSTRAP_SERVERS = 'localhost:9092'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
ORDERS_TOPIC = 'orders'
ORDERS_DLQ_TOPIC = 'orders_dlq'
CONSUMER_GROUP_ID = 'order-processor-group'
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1
ERROR_SIMULATION_RATE = 0.1
```

---

## 📊 System Behavior

### Message Flow
```
Producer → [orders topic] → Consumer
                              ├─ ✅ Success → Running Average
                              ├─ ⚠️ ConnectionError → Retry (1s, 2s, 4s)
                              │                      ├─ ✅ Success
                              │                      └─ ❌ Fail → [orders_dlq]
                              └─ ❌ ValueError → [orders_dlq] (no retry)
```

### Error Rates (Simulated)
- **Success Rate:** ~90%
- **ConnectionError:** ~5% (retryable with exponential backoff)
- **ValueError:** ~5% (non-retryable poison pills)

### Retry Strategy
- **Exponential Backoff:** 1s → 2s → 4s (total 7 seconds)
- **Max Retries:** 3 attempts
- **Backoff Formula:** `delay = RETRY_BACKOFF_SECONDS * (2 ^ (attempt - 1))`

### Running Average Algorithm
- **Type:** Incremental calculation
- **Time Complexity:** O(1) per update
- **Space Complexity:** O(1) - no storage of all values
- **Formula:** `average = sum / count`

---

## 🧪 Verification

### Check Kafka Topics
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Expected Output:**
```
__consumer_offsets
orders
orders_dlq
```

### Check Schema Registry
```bash
curl http://localhost:8081/subjects
```

**Expected Output:**
```json
["orders-value","orders_dlq-value"]
```

### View Schema Details
```bash
curl http://localhost:8081/subjects/orders-value/versions/1 | python -m json.tool
```

---

## 📁 Project Structure

```
Bigdata/
├── docker-compose.yml    # Kafka, Zookeeper, Schema Registry
├── order.avsc            # Avro schema definition
├── config.py             # Centralized configuration
├── producer.py           # Order message producer
├── consumer.py           # Consumer with retry & DLQ
├── dlq_monitor.py        # DLQ monitoring tool
├── requirements.txt      # Python dependencies
├── .venv/                # Python virtual environment
├── demo.mp4              # Live demonstration video
└── README.md             # This file
```

---

## 🔍 Troubleshooting

### Producer won't start
- Verify Docker is running: `docker compose ps`
- Check port 9092 is available: `lsof -i :9092`
- Ensure venv is activated: `which python`

### Consumer shows no messages
- Wait 30-60 seconds for Kafka to initialize
- Verify producer is running and sending messages
- Check topics exist: `docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092`

### Import errors
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### Schema Registry not responding
```bash
docker compose logs schema-registry
curl http://localhost:8081/subjects
```

---

## 🎯 Key Features Demonstrated

✅ **Real-time Stream Processing** - Process orders as they arrive  
✅ **Fault Tolerance** - System continues despite errors  
✅ **Retry Logic** - Exponential backoff for transient failures  
✅ **Dead Letter Queue** - Capture and investigate failed messages  
✅ **Schema Management** - Avro with Schema Registry  
✅ **Running Average** - Incremental real-time calculation  
✅ **Graceful Shutdown** - Proper offset commit and cleanup  
✅ **Observability** - Comprehensive logging and statistics

---

## 📝 Technical Details

**Languages & Frameworks:**
- Python 3.14
- confluent-kafka 2.12.2
- fastavro 1.12.1

**Infrastructure:**
- Apache Kafka 7.5.0
- Confluent Schema Registry 7.5.0
- Apache Zookeeper 7.5.0
- Docker Compose

**Patterns:**
- Producer-Consumer
- Retry with Exponential Backoff
- Dead Letter Queue (DLQ)
- Schema Evolution
- Consumer Groups
- At-least-once Delivery

---

## 📧 Support

For issues or questions, refer to the [demo video](./demo.mp4) for a complete walkthrough of the system in action.

---

**Status:** ✅ Production Ready
