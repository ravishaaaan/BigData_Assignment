"""
Configuration settings for Kafka Producer-Consumer system.
Centralized configuration for easy management across modules.
"""

# Kafka Broker Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'

# Schema Registry Configuration
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

# Topic Names
ORDERS_TOPIC = 'orders'
ORDERS_DLQ_TOPIC = 'orders_dlq'

# Consumer Configuration
CONSUMER_GROUP_ID = 'order-processor-group'
AUTO_OFFSET_RESET = 'earliest'  # Start from beginning if no committed offset

# Producer Configuration
PRODUCER_ACKS = 'all'  # Wait for all in-sync replicas

# Retry Configuration (for Step 6)
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1

# Error Simulation (for Step 5)
ERROR_SIMULATION_RATE = 0.1  # 10% chance of simulated errors
